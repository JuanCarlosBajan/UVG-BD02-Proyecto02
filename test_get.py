from Table import Table
from HBase import HBase
from HFile import HFile, Row


def test_get_rows():
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
		result = hbase.Get('test', 1, [
			'general:name',
		], 1)
		# Assert
		assert len(result) == 1
		assert result[0].key == 1
		assert result[0].column == 'general:name'
		assert result[0].timestamp == 1
		assert result[0].value == 'John'
		assert result[0].enabled == True


def test_get_rows_multiple():
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
		result = hbase.Get('test', 1, [
			'general:name',
			'general:age',
			'address:street',
			'address:city',
		], 1)
		# Assert
		assert len(result) == 4
		assert result[0].key == 1
		assert result[0].column == 'general:name'
		assert result[0].timestamp == 1
		assert result[0].value == 'John'
		assert result[0].enabled == True
		assert result[1].key == 1
		assert result[1].column == 'general:age'
		assert result[1].timestamp == 1
		assert result[1].value == 20
		assert result[1].enabled == True
		assert result[2].key == 1
		assert result[2].column == 'address:street'
		assert result[2].timestamp == 1
		assert result[2].value == '123 Main St'
		assert result[2].enabled == True
		assert result[3].key == 1
		assert result[3].column == 'address:city'
		assert result[3].timestamp == 1
		assert result[3].value == 'New York'
		assert result

if __name__ == '__main__':
	test_get_rows()
	test_get_rows_multiple()