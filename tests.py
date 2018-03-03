import unittest

from migrate import Product, Migrate, BaseReadDB, BaseOutputWriterDB


class ProductTestCase(unittest.TestCase):

    def test_create_product_instance(self):
        product = Product(description='desc', price=123)
        self.assertEqual(product.description, 'desc')
        self.assertEqual(product.price, 123)

    def test_check_product_true_equivalency(self):
        product1 = Product(
            description='COLÔNIA DESODORANTE AVON 015 LONDON',
            price=134
        )
        product2 = Product(
            description='cOlONiIâ DEZODORRANTE AVÃO 015 LONDON',
            price=123
        )
        self.assertEqual(product1, product2)

    def test_check_product_false_equivalency(self):
        product1 = Product(
            description='COLÔNIA DESODORANTE MUSK MARINE',
            price=123
        )
        product2 = Product(
            description='COLÔNIA DESODORANTE MUSK FRESH',
            price=134
        )
        self.assertNotEqual(product1, product2)


class MockReadDB1(BaseReadDB):
    STORE = []

    def read(self):
        for item in self.STORE:
            yield item

    def close(self):
        pass

class MockReadDB2(BaseReadDB):
    STORE = []

    def read(self):
        for item in self.STORE:
            yield item

    def close(self):
        pass


class MockOutputDB(BaseOutputWriterDB):
    STORE = []

    def writerow(self, row):
        self.STORE.append(row)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MigrateTestCase(unittest.TestCase):

    def test_migrate(self):
        mock_db1 = MockReadDB1('')
        mock_db1.STORE.append(
            ['COLÔNIA DESODORANTE AVON 300 KM/H MAX TURBO', 250])
        mock_db1.STORE.append(['AVON LUCK FOR HIM DEO PARFUM', 50])

        mock_db2 = MockReadDB2('')
        mock_db2.STORE.append(
            ['cOlONiIâ DEZODORRANTE AVÃO 300 KM/H MAX TURBO', 100])
        mock_db2.STORE.append(['AVÃO luck for him deo parfum', 124])

        migrate = Migrate(mock_db1, mock_db2)

        output_db_mock = MockOutputDB('')

        migrate.run(output_db=output_db_mock)

        self.assertListEqual(
            output_db_mock.STORE[0],
            ['COLÔNIA DESODORANTE AVON 300 KM/H MAX TURBO', 100]
        )
        self.assertListEqual(
            output_db_mock.STORE[1],
            ['AVON LUCK FOR HIM DEO PARFUM', 124],
        )


if __name__ == '__main__':
    unittest.main()
