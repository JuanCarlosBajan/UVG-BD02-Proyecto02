# Proyecto 02 - Bases de Datos 02

## Comandos - Definición de datos
- Create: `create 'users','general','address'`
- Is_Enabled: `is_enabled 'users'`
- Alter: `alter 'users', {NAME => 'usuarios'}`
- Drop: ``
- Drop All: ``
- Describe: ``
- List: `list`

## Comandos - Manipulación de datos
- Put: `put 'test',2,'general:name','Peter'`
- Get: `get 'test',1,{COLUMN => 'general:name'}`
- Scan: `scan 'test'`, `scan 'test',{LIMIT=>2}`, `scan 'test',{STARTROW=>2,ENDROW=>4}`
- Delete: `delete 'test',1,general:name`
- DeleteAll: `deleteall 'test',1`
- Count: `count 'test'`
- Truncate: `truncate 'test'`