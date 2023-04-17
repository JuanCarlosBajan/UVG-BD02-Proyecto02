from Table import Table
from HBase import HBase
from HFile import HFile, Row


def test_scan():
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
    result = hbase.Scan('test')
    # Assert
    assert result[0].column == 'general:name'
    assert result[0].value == 'John'
    assert result[0].enabled == True
    assert result[1].column == 'general:age'
    assert result[1].value == 20
    assert result[1].enabled == True
    assert result[2].column == 'address:street'
    assert result[2].value == '123 Main St'
    assert result[2].enabled == True
    assert result[3].column == 'address:city'
    assert result[3].value == 'New York'
    assert result[3].enabled == True

def test_scan_range():
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
        Row(2, 'general:name', 1, 'Joshua'),
        Row(2, 'general:age', 1, 25),
        Row(3, 'general:name', 1, 'Sofia'),
        Row(3, 'general:age', 1, 22),
        Row(4, 'general:name', 1, 'Rose'),
        Row(4, 'general:age', 1, 23),
        Row(5, 'general:name', 1, 'Lily'),
        Row(5, 'general:age', 1, 24),
    ], 'general')
    # For address info
    hfile2 = HFile([
        Row(1, 'address:street', 1, '123 Main St'),
        Row(1, 'address:city', 1, 'New York'),
        Row(2, 'address:street', 1, '456 Main St'),
        Row(2, 'address:city', 1, 'San Francisco'),
        Row(3, 'address:street', 1, '789 Main St'),
        Row(3, 'address:city', 1, 'Miami'),
        Row(4, 'address:street', 1, '101 Main St'),
        Row(4, 'address:city', 1, 'Los Angeles'),
        Row(5, 'address:street', 1, '102 Main St'),
        Row(5, 'address:city', 1, 'Chicago'),
    ], 'address')
    hbase.tables['test'].h_files.append(hfile)
    hbase.tables['test'].h_files.append(hfile2)
    # Act
    result = hbase.Scan('test', 2, 4)
    
    # Assert
    assert result[0].column == 'general:name'
    assert result[0].value == 'Joshua'
    assert result[0].enabled == True

    assert result[1].column == 'general:age'
    assert result[1].value == 25
    assert result[1].enabled == True

    assert result[2].column == 'general:name'
    assert result[2].value == 'Sofia'
    assert result[2].enabled == True

    assert result[3].column == 'general:age'
    assert result[3].value == 22
    assert result[3].enabled == True

    assert result[4].column == 'address:street'
    assert result[4].value == '456 Main St'
    assert result[4].enabled == True

    assert result[5].column == 'address:city'
    assert result[5].value == 'San Francisco'
    assert result[5].enabled == True

    assert result[6].column == 'address:street'
    assert result[6].value == '789 Main St'
    assert result[6].enabled == True

    assert result[7].column == 'address:city'
    assert result[7].value == 'Miami'
    assert result[7].enabled == True

def test_scan_limit():
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
		Row(2, 'general:name', 1, 'Joshua'),
		Row(2, 'general:age', 1, 25),
		Row(3, 'general:name', 1, 'Sofia'),
		Row(3, 'general:age', 1, 22),
		Row(4, 'general:name', 1, 'Rose'),
		Row(4, 'general:age', 1, 23),
		Row(5, 'general:name', 1, 'Lily'),
		Row(5, 'general:age', 1, 24),
	], 'general')
	# For address info
    hfile2 = HFile([
		Row(1, 'address:street', 1, '123 Main St'),
		Row(1, 'address:city', 1, 'New York'),
		Row(2, 'address:street', 1, '456 Main St'),
		Row(2, 'address:city', 1, 'San Francisco'),
		Row(3, 'address:street', 1, '789 Main St'),
		Row(3, 'address:city', 1, 'Miami'),
		Row(4, 'address:street', 1, '101 Main St'),
		Row(4, 'address:city', 1, 'Los Angeles'),
		Row(5, 'address:street', 1, '102 Main St'),
		Row(5, 'address:city', 1, 'Chicago'),
	], 'address')
    hbase.tables['test'].h_files.append(hfile)
    hbase.tables['test'].h_files.append(hfile2)

    # Act
    result = hbase.Scan('test', limit=2)
    # Assert
    assert result[0].column == 'general:name'
    assert result[0].value == 'John'
    assert result[0].key == 1

    assert result[1].column == 'general:age'
    assert result[1].value == 20
    assert result[1].key == 1

    assert result[2].column == 'general:name'
    assert result[2].value == 'Joshua'
    assert result[2].key == 2

    assert result[3].column == 'general:age'
    assert result[3].value == 25
    assert result[3].key == 2

    assert result[4].column == 'address:street'
    assert result[4].value == '123 Main St'
    assert result[4].key == 1

    assert result[5].column == 'address:city'
    assert result[5].value == 'New York'
    assert result[5].key == 1

    assert result[6].column == 'address:street'
    assert result[6].value == '456 Main St'
    assert result[6].key == 2

    assert result[7].column == 'address:city'
    assert result[7].value == 'San Francisco'
    assert result[7].key == 2




if __name__ == '__main__':
    test_scan_limit()