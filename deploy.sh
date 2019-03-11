export APP_NAME=salesplusimport-d
func azure functionapp publish $APP_NAME --build-native-deps
export APP_NAME=salesplusimport-t

func azure functionapp publish $APP_NAME --build-native-deps
export APP_NAME=salesplusimport-p
func azure functionapp publish $APP_NAME --build-native-deps
