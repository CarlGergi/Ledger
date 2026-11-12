// Ledger on Azure Container Apps for subscriptions that get only express environments,
// such as Azure for Students. The same gateway and database as main.bicep, without what
// express refuses:
//
// - Application Insights and the managed OpenTelemetry agent: the containers' output
//   goes to Log Analytics, and traces go wherever [telemetry] otlp_endpoint points.
// - Secret volumes: the config is a secret environment variable, written to a file at
//   start.
// - Sidecars: the example file tool server runs in the gateway's container.
// - A termination grace period: the platform's default applies.

@description('Lowercase letters and digits; names every resource. The same as for registry.bicep.')
@minLength(3)
@maxLength(16)
param namePrefix string

param location string = resourceGroup().location

@description('The registry from registry.bicep.')
param registryName string

@description('The image to run, such as myacr.azurecr.io/ledger:abc1234.')
param image string

@description('The MCP tool servers to put behind the gateway: [{ "name": "...", "url": "..." }].')
param upstreams array

@description('Also run the example file tool server beside the gateway, as the upstream "files", to try the deployment with no tool servers of your own.')
param exampleFiles bool = false

@description('An OpenAI-compatible base URL to proxy model calls to; empty for none.')
param modelUrl string = ''

param postgresAdmin string = 'ledger'

@secure()
param postgresPassword string

@description('One copy: an MCP transport session lives in the memory of the copy that opened it, and Container Apps cannot route by Mcp-Session-Id.')
@minValue(1)
param replicas int = 1

var suffix = uniqueString(resourceGroup().id)
var appName = '${namePrefix}-ledger'

resource workspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: '${namePrefix}-logs-${suffix}'
  location: location
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
  }
}

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' existing = {
  name: registryName
}

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: '${namePrefix}-ledger-identity'
  location: location
}

// AcrPull, so the app can pull its image without registry passwords.
resource pull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(registry.id, identity.id, 'acrpull')
  scope: registry
  properties: {
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      '7f951dda-4ed3-4680-a7ca-43fe172d538d'
    )
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource postgres 'Microsoft.DBforPostgreSQL/flexibleServers@2024-08-01' = {
  name: '${namePrefix}-pg-${suffix}'
  location: location
  sku: {
    name: 'Standard_B1ms'
    tier: 'Burstable'
  }
  properties: {
    version: '17'
    administratorLogin: postgresAdmin
    administratorLoginPassword: postgresPassword
    storage: { storageSizeGB: 32 }
    backup: {
      backupRetentionDays: 7
      geoRedundantBackup: 'Disabled'
    }
    highAvailability: { mode: 'Disabled' }
    network: { publicNetworkAccess: 'Enabled' }
  }
}

// Connections from Azure services only, the app among them. TLS is required by default.
resource azureServices 'Microsoft.DBforPostgreSQL/flexibleServers/firewallRules@2024-08-01' = {
  parent: postgres
  name: 'AllowAzureServices'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

resource database 'Microsoft.DBforPostgreSQL/flexibleServers/databases@2024-08-01' = {
  parent: postgres
  name: 'ledger'
}

resource environment 'Microsoft.App/managedEnvironments@2025-07-01' = {
  name: '${namePrefix}-env-${suffix}'
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: workspace.properties.customerId
        sharedKey: workspace.listKeys().primarySharedKey
      }
    }
  }
}

var fqdn = '${appName}.${environment.properties.defaultDomain}'
var allUpstreams = concat(upstreams, exampleFiles ? [ { name: 'files', url: 'http://127.0.0.1:7102/mcp' } ] : [])
var upstreamToml = join(
  map(allUpstreams, u => '[[upstream]]\nname = "${u.name}"\nurl = "${u.url}"\n'),
  '\n'
)
var modelToml = empty(modelUrl) ? '' : '\n[model]\nurl = "${modelUrl}"\n'
var config = 'listen = "0.0.0.0:7100"\nallowed_hosts = ["${fqdn}"]\nshutdown_grace_secs = 25\n\n${upstreamToml}${modelToml}'
var writeConfig = 'printf \'%s\' "$LEDGER_TOML" > /tmp/ledger.toml'
var exampleServer = exampleFiles ? ' && (file-server --listen 127.0.0.1:7102 --root /tmp/files &)' : ''
var startup = '${writeConfig}${exampleServer} && exec ledger serve --config /tmp/ledger.toml'
var databaseUrl = 'postgres://${postgresAdmin}:${uriComponent(postgresPassword)}@${postgres.properties.fullyQualifiedDomainName}:5432/${database.name}?sslmode=require'

resource app 'Microsoft.App/containerApps@2025-07-01' = {
  name: appName
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: { '${identity.id}': {} }
  }
  dependsOn: [ pull, azureServices ]
  properties: {
    managedEnvironmentId: environment.id
    configuration: {
      ingress: {
        external: true
        targetPort: 7100
        transport: 'http'
        allowInsecure: false
      }
      registries: [
        {
          server: registry.properties.loginServer
          identity: identity.id
        }
      ]
      secrets: [
        { name: 'database-url', value: databaseUrl }
        { name: 'ledger-toml', value: config }
      ]
    }
    template: {
      // One container. Its config arrives as a secret variable, written to a file at start;
      // `exec` makes the gateway the container's main process, so it gets SIGTERM and
      // drains. With exampleFiles, the example tool server runs beside it on loopback.
      // Express environments, which some subscriptions are given whatever is asked for,
      // allow no sidecars, no secret volumes and no termination grace period.
      containers: [
        {
          name: 'ledger'
          image: image
          command: [ 'sh', '-c', startup ]
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
          env: [
            { name: 'DATABASE_URL', secretRef: 'database-url' }
            { name: 'LEDGER_TOML', secretRef: 'ledger-toml' }
          ]
          probes: [
            {
              type: 'Liveness'
              httpGet: { path: '/healthz', port: 7100 }
              periodSeconds: 10
            }
            {
              type: 'Readiness'
              httpGet: { path: '/readyz', port: 7100 }
              periodSeconds: 5
            }
          ]
        }
      ]
      scale: {
        minReplicas: replicas
        maxReplicas: replicas
      }
    }
  }
}

output mcpUrl string = 'https://${fqdn}/mcp'
output modelUrl string = 'https://${fqdn}/v1'
output postgresHost string = postgres.properties.fullyQualifiedDomainName
output logsWorkspace string = workspace.name
