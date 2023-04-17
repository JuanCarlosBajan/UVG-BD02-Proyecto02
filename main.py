
from HBase import HBase
import csv


print("Bienvenido al simulador de Hbase")

hbase = HBase()

hbase.Create_Test_Table()
hbase.Load_DataSet()

def parse_get_command(cmd):
    parts = cmd.split(',')
    table_name = parts[0].strip()[4:]
    row_key = parts[1].strip()
    column_spec = parts[2].strip()
    if 'COLUMN' in column_spec:
        column_spec_parts = column_spec.split('=>')[1].strip()
        columns = column_spec_parts.split(',')
        columns = [c.strip() for c in columns]
        columns = [c.strip('{}').strip() for c in columns]
        columns = [c.split(':') for c in columns]
        columns = [f"{cf}:{col}" for cf, col in columns]
        columns_str = ",".join(columns)
    else:
        columns_str = None
    if 'VERSIONS' in column_spec:
        versions = column_spec.split('=>')[2].strip()
    else:
        versions = None
    return (table_name, row_key, columns_str, versions)







while True:
		try:
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
							print(">> " + str(name))

			elif command[0] == "disable":
					table_name = command[1][1:-1]
					if not hbase.Disable(table_name):
							print(">> La tabla '" + table_name + "' no existe")
					else:
							print(">> La tabla '" + table_name + "' se ha deshabilitado")

			elif command[0] == "enable":
					table_name = command[1][1:-1]
					if not hbase.Enable(table_name):
							print(">> La tabla '" + table_name + "' no existe")
					else:
							print(">> La tabla '" + table_name + "' se ha habilitado")

			elif command[0] == "is_enabled":
					table_name = command[1][1:-1]
					print(">> " + str(hbase.Is_Enabled(table_name)))

			elif command[0] == "alter":
					table_name = command[1][1:-2]
					content = " ".join(command[2:])
					keys = [-1,-1]
					for i in range(0, len(content)):
							if content[i] == "{":
									keys[0] = i
							if content[i] == "}":
									keys[1] = i
					content = content[keys[0]+1:keys[1]]
					content = content.split(" => ")

					if len(content) != 2:
							print(">> Error con el comando")

					elif content[0] == "NAME":

							if hbase.Alter_Table_Name(table_name, content[1][1:-1]):
									print(">> Se ha modificado la tabla '" + table_name + "' a '" + content[1][1:-1] + "'")

							else:
									print(">> Ha ocurrido un error")

					elif content[0] == "ADD":
							if ':' not in content[1][1:-1]:
								if hbase.Alter_Table_Add(table_name, content[1][1:-1]):
										print(">> Se ha agregado la familia '" + content[1][1:-1] + "' a la tabla '" + table_name + "'")

								else:
										print(">> Ha ocurrido un error")
							else:
								column_family, column = content[1][1:-1].split(':')
								if hbase.Alter_Table_Add_Column(table_name, column_family, column):
									print(">> Se ha agregado la columna '" + column + "' a la familia '" + column_family + "' de la tabla '" + table_name + "'")
								else:
									print(">> Ha ocurrido un error")

					elif content[0] == "DELETE":
							if hbase.Alter_Table_Delete(table_name, content[1][1:-1]):
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
					content = command[1][1:-1]
					res = hbase.Describe(content)
					if not res:
						print(">> Error al realizar describe de esta tabla")
			
			elif command[0] == "deleteall":
				# example deleteall 'test',1
				command[1].replace("'","")
				content = "".join(command[1:])
				content = content.split(",")
				table_name = content[0]
				row_key = None
				if len(content) < 2:
					print(">> Error con el comando")
					continue
				row_key = content[1]
				total_deleted = hbase.Delete(table_name, row_key)
				print(">> Se han eliminado " + str(total_deleted) + " registros")
			
			elif command[0] == "get":
				# example get 'test',1,{COLUMN => 'general:name', VERSION => 1}
				hbase.Create_Test_Table()
				command[1].replace("'","")
				content = "".join(command[1:])
				content = content.split(",")
				table_name = content[0].replace("'","")
				if len(content) < 3:
					print(">> Error con el comando")
					continue
				
				row_key = content[1].replace("'","")
				extra = ",".join(content[2:])
				extra = extra.replace("'","")
				extra = extra.replace(" ","")
				extra = extra.replace("{","")
				extra = extra.replace("}","")
				parameters = extra.split(",")
				column_family = None
				column = None
				version = 1
				for p in parameters:
					key, value = p.split("=>")
					if key == "COLUMN":
						column_family, column = value.split(":")
					if key == "VERSION":
						version = int(value)
				
				rows = hbase.Get(table_name, row_key,[column_family + ":" + column], version)
				if rows is None or len(rows) == 0:
					print(">> No se encontraron registros")
				else:
					for row in rows:
						print(">> Key:" + str(row.key))
						print(">> Value:" + str(row.value))
						print(">> Timestamp:" + str(row.timestamp))

				

			elif command[0] == "truncate":
				command[1].replace("'","")
				content = "".join(command[1:])
				content = content.split(",")
				table_name = content[0]
				if hbase.Truncate(table_name):
					print(">> Se ha truncado la tabla '" + table_name + "'")
				else:
					print(">> Ha ocurrido un error")	
			
			elif command[0] == "delete":
				command[1].replace("'","")
				multiple_keys = False
				keys_found = ''
				for c in command[1]:		
					if multiple_keys and c != '}':
						keys_found += c
					if c == "{":
						multiple_keys = True

				content = "".join(command[1:])
				content = content.split(",")
				table_name = content[0]
				row_key = None
				column_identifier = None
				timestamp = None
				if len(content) < 2:
					print(">> Error con el comando")
					continue
				row_key = content[1]
				column_name = None
				column_family = None
				if len(content) >= 3:
					column_identifier = content[2]
					if ":" in column_identifier:
						column_family = column_identifier.split(":")[0]
						column_name = column_identifier.split(":")[1]
					else:
						column_family = column_identifier
				if len(content) == 4 and not multiple_keys:
					timestamp = content[3]
				if not multiple_keys:
					total_deleted = hbase.Delete(table_name, row_key,  column_family, column_name, timestamp)
				else:
					splitted_keys = keys_found.split(',')
					total_deleted = 0
					for key in splitted_keys:
						if ":" in key:
							column_family = key.split(":")[0]
							column_name = key.split(":")[1]
						else:
							column_family = key
							column_name = None
						total_deleted += hbase.Delete(table_name, row_key,  column_family, column_name, timestamp)
				print(">> Se han eliminado " + str(total_deleted) + " registros")

			#AQUI DEBEN IR TODOS LOS DEMAS COMANDOS

			elif command[0] == "put":
					# example put 'test',2,'general:name','Peter'
					hbase.Create_Test_Table()
					command[1].replace("'","")
					arguments = command[1].split(",")
					if len(arguments) < 4:
							print(">> Error con el comando")
							continue
					table_name = arguments[0][1:-1]
					row = arguments[1]
					column_family = arguments[2][1:-1].split(':')[0]
					column_name = arguments[2][1:-1].split(':')[1]
					value = arguments[3][1:-1]
					timestamp = arguments[4] if len(arguments) == 5 else None

					if hbase.Put(table_name, row, column_family, column_name, value, timestamp):
						print(">> Se ha insertado el registro")
					else:
						print(">> Ha ocurrido un error")

			elif command[0] == "count":
				hbase.Create_Test_Table()
				command[1].replace("'","")
				arguments = command[1].split(",")
				if len(arguments) < 1:
						print(">> Error con el comando")
						continue
				table_name = arguments[0][1:-1]

				if table_name not in hbase.tables.keys():
					print(">> La tabla '" + table_name + "' no existe")
					continue
				else:
					if not hbase.Count(table_name):
						print(">> La tabla '" + table_name + "' no tiene registros")
					else:
						print(">> La tabla '" + table_name + "' tiene " + str(hbase.Count(table_name)) + " registros")

			elif command[0] == "scan":
				hbase.Create_Test_Table2()
				command[1].replace("'","")
				arguments = command[1].split(",")
				if len(arguments) < 1:
						print(">> Error con el comando")
						continue
				table_name = arguments[0][1:-1]

				lenn = len(arguments)
				start, end, limit = None, None, None
				if len(arguments) == 3:
					if arguments[1][0] != '{' and arguments[2][-1] != '}':
						print(">> Error con el comando")
						continue
					else:
						arg1 = arguments[1][1:]
						arg2 = arguments[2][:-1]

						if '=>' not in arg1 or '=>' not in arg2:
							print(">> Error con el comando")
							continue
						else:
							start = arg1.split('=>')[1]
							end = arg2.split('=>')[1]
							start = int(start)
							end = int(end)
							keyword_start = arg1.split('=>')[0]
							keyword_end = arg2.split('=>')[0]
							if keyword_start != 'STARTROW' or keyword_end != 'ENDROW':
								print(">> Error con el comando")
								continue

				if len(arguments) == 2:
					if arguments[1][0] != '{' and arguments[1][-1] != '}':
						print(">> Error con el comando")
						continue
					else:
						arg1 = arguments[1][1:-1]
						if '=>' not in arg1:
							print(">> Error con el comando")
							continue
						else:
							limit = arg1.split('=>')[1]
							limit = int(limit)
							keyword_lim = arg1.split('=>')[0]
							if keyword_lim != 'LIMIT':
								print(">> Error con el comando")
								continue	

				if table_name not in hbase.tables.keys():
					print(">> La tabla '" + table_name + "' no existe")
					continue
				else:
					if start and end:
						if start > end:
							print(">> Error con el comando, rangos no validos")
							continue
						elif not hbase.Scan(table_name=table_name, row_start=start, row_stop=end):
							print(">> La tabla '" + table_name + "' no tiene registros en ese rango")
						else:
							for row in hbase.Scan(table_name=table_name, row_start=start, row_stop=end):
								print(" Key:" + str(row.key) + " value:" + str(row.value) + " timestamp:" + str(row.timestamp))
					else:
						if limit:
							if not hbase.Scan(table_name=table_name, limit=limit):
								print(">> La tabla '" + table_name + "' no tiene registros con las especificaciones dadas")
							else:
								for row in hbase.Scan(table_name=table_name, limit=limit):
									print(" Key:" + str(row.key) + " value:" + str(row.value) + " timestamp:" + str(row.timestamp))
						else:
							if not hbase.Scan(table_name):
								print(">> La tabla '" + table_name + "' no tiene registros con las especificaciones dadas")
							else:
								for row in hbase.Scan(table_name):
									print(" Key:" + str(row.key) + " value:" + str(row.value) + " timestamp:" + str(row.timestamp))

							
				

					
			else:
					print("comando '" + command[0] + "' No aceptado")
		except Exception as e:
			print(">> Comando no reconocido ", e)