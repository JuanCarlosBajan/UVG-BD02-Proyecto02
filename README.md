# Proyecto 02 - Bases de Datos 02

## Comandos - Definición de datos
- Create: `create 'users','general','address'`
- Is_Enabled: `is_enabled 'users'`
- Alter: `alter 'users', {NAME => 'usuarios'}`, `alter 'usuarios', {ADD => 'familia_prueba'}`

- Drop: ``
- Drop All: ``
- Describe: ``
- List: `list`

## Comandos - Manipulación de datos
- Put: `put 'test',2,'general:name','Peter'`
- Get: `get initial,1,{COLUMN=>'general:title'}`
- Scan: `scan 'test'`, `scan 'test',{LIMIT=>2}`, `scan 'test',{STARTROW=>2,ENDROW=>4}`, `scan 'initial',{LIMIT=>2}`
- Delete: `delete 'initial',1`
- DeleteAll: `deleteall 'initial',1`
- Count: `count 'initial'`
- Truncate: `truncate 'initial'`