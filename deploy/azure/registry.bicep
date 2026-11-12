// A container registry for the gateway's image. Deployed first, so that the image can be
// pushed to it before the app that runs it exists.

@description('Lowercase letters and digits; names every resource.')
@minLength(3)
@maxLength(16)
param namePrefix string

param location string = resourceGroup().location

resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: '${namePrefix}acr${uniqueString(resourceGroup().id)}'
  location: location
  sku: { name: 'Basic' }
  properties: {
    adminUserEnabled: false
  }
}

output name string = registry.name
output loginServer string = registry.properties.loginServer
