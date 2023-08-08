def yamlGetter(config_file):
    print("Inside yamlGetter() function")
    file = open(config_file, "r")
    print(file.read())
