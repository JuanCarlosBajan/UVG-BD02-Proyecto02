
from HBase import HBase


print("Bienvenido al simulador de Hbase")

hbase = HBase()

while True:
    command = input(">>")

    if command == "exit()":
        print("saliendo...")
        break

    command = command.split(" ")

    while "" in command:
        command.remove("")
    
    if command[0] == "create":
        content = "".join(command[1:])
        content = content.split(",")
        table_name = content[0][1:-1]
        table_columns_familys = content[1:]
        for i in range(0,len(table_columns_familys)):
            table_columns_familys[i] = table_columns_familys[i][1:-1]
        
        if not hbase.Create(table_name,table_columns_familys):
            print(">> La tabla '" + table_name + "' no existe")
        else:
            print(">> La tabla '" + table_name + "' se ha creado")



    elif command[0] == "list":
        for name in hbase.List():
            print(">> " + name)

    elif command[0] == "disable":
        table_name = command[1][1:-1]
        if not hbase.Disable(table_name):
            print(">> La tabla '" + table_name + "' no existe")
        else:
            print(">> La tabla '" + table_name + "' se ha deshabilitado")

    elif command[0] == "is_enabled":
        table_name = command[1][1:-1]
        print(">> " + str(hbase.Is_Enabled(table_name)))

    elif command[0] == "alter":
        table_name = command[1][1:-1]
        content = "".join(command[2:])
        keys = [-1,-1]
        for i in range(0, len(content)):
            if content[i] == "{":
                keys[0] = i
            if content[i] == "}":
                keys[1] = i
        content = content[keys[0]+1,keys[1]]
        content = content.split(" => ")

        if len(content) != 2:
            print(">> Error con el comando")

        elif content[0] == "NAME":

            if hbase.Alter_Table_Name(table_name, content[1][1:-1]):
                print(">> Se ha modificado la tabla '" + table_name + "' a '" + content[1][1:-1] + "'")

            else:
                print(">> Ha ocurrido un error")

        elif content[0] == "ADD":
            if hbase.Alter_Table_Add(table_name, content[1][1:-1]):
                print(">> Se ha agregado la familia '" + content[1][1:-1] + "' a la tabla '" + table_name + "'")

            else:
                print(">> Ha ocurrido un error")

        elif content[0] == "DELETE":
            if hbase.Alter_Table_Add(table_name, content[1][1:-1]):
                print(">> Se ha eliminado la familia '" + content[1][1:-1] + "' a la tabla '" + table_name + "'")

            else:
                print(">> Ha ocurrido un error")

    elif command[0] == "drop":
        table_name = command[1][1:-1]

        if hbase.Delete_Table(table_name):
            print(">> Se ha eliminado la tabla '" + table_name + "'")
        
        else:
            print(">> Ha ocurrido un error (recuerda que para eliminar una tabla debes desabilitarla primero)")

    elif command[0] == "drop_all":
        if hbase.Delete_Table_All():
            print(">> Se ha eliminado todas las tablas")
        
        else:
            print(">> Ha ocurrido un error ")

    elif command[0] == "describe":
        pass
    

    #AQUI DEBEN IR TODOS LOS DEMAS COMANDOS

    elif command[0] == "put":
        print("comando put")

    else:
        print("comando '" + command[0] + "' No aceptado")