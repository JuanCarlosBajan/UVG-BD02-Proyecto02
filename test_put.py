from Table import Table
from HBase import HBase
from HFile import HFile, Row


def test_put():
    # Arrange
		hbase = HBase()
		hbase.tables['test'] = Table('users', ['general', 'address'])
		hbase.tables['test'].add_column('general', 'name')
		hbase.tables['test'].add_column('general', 'age')
		hbase.tables['test'].add_column('address', 'street')
		hbase.tables['test'].add_column('address', 'city')
		
		hfile = HFile([
			Row(1, 'general:name', 1, 'John'),
			Row(1, 'general:age', 1, 20),
		], 'general')
		# For address info
		hfile2 = HFile([
			Row(1, 'address:street', 1, '123 Main St'),
			Row(1, 'address:city', 1, 'New York'),
		], 'address')
		hbase.tables['test'].h_files.append(hfile)
		hbase.tables['test'].h_files.append(hfile2)
		# Act
        # table_name, row_key, column_family, column_name, value
		result = hbase.Put('test', 2, 'general' , 'name', 'Charlie')
		# Assert
		assert result == True
		assert hbase.tables['test'].h_files[0].rows[-1].column == 'general:name'
		assert hbase.tables['test'].h_files[0].rows[-1].value == 'Charlie'
		assert hbase.tables['test'].h_files[0].rows[-1].enabled == True
                
def test_put_no_hfile():
        # Arrange
		hbase = HBase()
		hbase.tables['test'] = Table('users', ['general', 'address'])
		hbase.tables['test'].add_column('general', 'name')
		hbase.tables['test'].add_column('general', 'age')
		hbase.tables['test'].add_column('address', 'street')
		hbase.tables['test'].add_column('address', 'city')
		
        # Act
		result = hbase.Put('test', 1, 'address' , 'city', 'New York')
		# Assert
		assert result == True
		assert hbase.tables['test'].h_files[0].rows[-1].column == 'address:city'
		assert hbase.tables['test'].h_files[0].rows[-1].value == 'New York'
		assert hbase.tables['test'].h_files[0].rows[-1].enabled == True
		

if __name__ == '__main__':
    test_put()
    test_put_no_hfile()