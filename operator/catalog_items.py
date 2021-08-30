import utils


class CatalogItems(object):

    def __init__(self, logger, prov_data):
        self.debug = False
        self.logger = logger
        self.prov_data = prov_data

    def check_catalog_exists(self):
        query = f"SELECT id FROM catalog_items " \
                f"WHERE catalog_item = '{self.prov_data['catalog_item']}'"
        result = utils.execute_query(query)
        if result['rowcount'] >= 1:
            query_result = result['query_result'][0]
            return query_result.get('id')
        else:
            return -1

    def populate_catalog_items(self):
        c_type = 'Dedicated'
        if 'SHARED' in self.prov_data['class_name']:
            c_type = 'Shared'
        elif 'sandbox' in self.prov_data['account']:
            c_type = 'Sandbox'

        # If catalog Item doesn't exists, we have to insert
        catalog_id = self.check_catalog_exists()
        if catalog_id == -1:
            query = f"INSERT INTO catalog_items ( \n" \
                    f"  catalog_item, \n" \
                    f"  catalog_name, \n" \
                    f"  class_name, \n" \
                    f"  infra_type) \n" \
                    f"VALUES ( \n" \
                    f"  '{self.prov_data['catalog_item']}', \n" \
                    f"  '{self.prov_data.get('catalog_name', '')}', \n" \
                    f"  '{self.prov_data['class_name']}', \n" \
                    f"  '{c_type}') RETURNING id \n"
            if self.debug:
                self.logger.info(f"Inserting Catalog Item: \n{query}")
            result = utils.execute_query(query, autocommit=True)
            if result['rowcount'] >= 1:
                query_result = result['query_result'][0]
                return query_result.get('id')
            else:
                self.logger.error(f"Error inserting catalog {self.prov_data['catalog_item']}")
                return -1
        else:
            # TODO: Needs to be updated or just return the catalog_id???
            return catalog_id
