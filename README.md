# Proyecto 02 - Bases de Datos 02

## Comandos - Definición de datos
- Create: ``
- Is_Enabled: ``
- Alter
- Drop
- Drop All
- Describe

## Comandos - Manipulación de datos
- Put: 
- Get: `get 'test',1,{COLUMN => 'general:name'}`
- Scan: `scan 'test'`, `scan 'test',{LIMIT=>2}`, `scan 'test',{STARTROW=>2,ENDROW=>4}`
- Delete: `delete 'test',1,general:name`
- DeleteAll: `deleteall 'test',1`
- Count: `count 'test'`
- Truncate: `truncate 'test'`