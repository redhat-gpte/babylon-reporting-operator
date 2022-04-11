import utils
from datetime import datetime, timezone


class CatalogItems(object):

    def __init__(self, logger, prov_data):
        self.debug = False
        self.logger = logger
        self.prov_data = prov_data

    def populate_catalog_items(self):
        catalog_type = 'Dedicated'
        if 'SHARED' in self.prov_data.get('class_name'):
            catalog_type = 'Shared'
        elif 'sandbox' in self.prov_data['account']:
            catalog_type = 'Sandbox'

        catalog_item = self.prov_data.get('catalog_item')
        catalog_name = self.prov_data.get('catalog_name', '')
        class_name = self.prov_data.get('class_name', None)

        insert_fields = {
            'catalog_item': catalog_item,
            'catalog_name': catalog_name,
            'class_name': class_name,
            'infra_type': catalog_type,
            'created_at': datetime.now(timezone.utc),
            'updated_at': datetime.now(timezone.utc),
        }

        update_fields = {
            'class_name': class_name,
            'infra_type': catalog_type,
            'updated_at': datetime.now(timezone.utc),
        }

        query, positional_args = utils.create_sql_statement(insert_fields=insert_fields,
                                                            update_fields=update_fields,
                                                            table_name='catalog_items',
                                                            constraint='catalog_items_unique',
                                                            return_field='id')

        if self.debug:
            print(f"Query Insert: \n{query}")

        cur = utils.execute_query(query, positional_args=positional_args, autocommit=True)

        if cur['rowcount'] >= 1:
            query_result = cur['query_result'][0]
            return query_result.get('id')
        else:
            return None
