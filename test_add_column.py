
from Table import Table

def test_add_column():
    # Arrange
		table = Table("users", ["cf1", "cf2"])
		# Act
		table.add_column("cf1", "name")
		# Assert
		print(table.family_columns)
		assert table.family_columns["cf1"] == ["name"]