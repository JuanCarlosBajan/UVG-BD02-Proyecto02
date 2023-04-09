
from table import Table


class HBase:

    def __init__(self):
        self.tables = {}

    def Create(self, name, family_columns):
        if name not in self.tables.keys:
            table = Table(name, family_columns)
            self.tables[name] = table
            return True
        return False
    
    def List(self):
        return self.tables.keys
    
    def Disable(self, name):
        if name in self.tables.keys:
            self.tables[name].disable()
            return True
        return False
    
    def Is_Enabled(self, name):
        if name in self.tables.keys:
            return self.tables[name].is_enabled()
        return False
    
    def Alter_Table_Name(self,table, new_name):
        found_old = table in self.tables.keys
        found_new = new_name in self.tables.keys
        
        if found_old and not found_new:
            old_table = self.tables[table]
            del self.tables[table]
            old_table.change_name(new_name)
            self.tables[found_new] = old_table
            return True
        
        return False
    
    def Alter_Table_Add(self, name, column_family):
        if name in self.tables.keys:
            return self.tables[name].add_family(column_family)
        return False
    
    def Alter_Table_Delete(self, name, column_family):
        if name in self.tables.keys:
            return self.tables[name].delete_family(column_family)
        return False
    
    def Delete_Table(self, name):
        if name in self.tables.keys and not self.tables[name].is_enabled():
            del self.tables[name]
            return True
        return False
    
    def Delete_Table_All(self):
        self.tables = {}
        return True