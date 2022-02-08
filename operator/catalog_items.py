import utils


class CatalogItems(object):

    def __init__(self, logger, prov_data):
        self.debug = False
        self.logger = logger
        self.prov_data = prov_data

    def check_catalog_exists(self):
        positional_args = [self.prov_data.get('catalog_item')]
        query = f"SELECT id FROM catalog_items \n" \
                f"WHERE catalog_item = %s \n" \
                f"LIMIT 1;"

        try:
            result = utils.execute_query(query, positional_args=positional_args, autocommit=True)

            if result['rowcount'] >= 1:
                query_result = result['query_result'][0]
                return query_result.get('id')
            else:
                return -1
        except Exception:
            self.logger.error("Error validating catalog item", stack_info=True)

    def populate_catalog_items(self):
        c_type = 'Dedicated'
        if 'SHARED' in self.prov_data['class_name']:
            c_type = 'Shared'
        elif 'sandbox' in self.prov_data['account']:
            c_type = 'Sandbox'

        catalog_id = self.check_catalog_exists()

        if catalog_id == -1:
            positional_args = [self.prov_data['catalog_item'],
                               self.prov_data.get('catalog_name', ''),
                               self.prov_data['class_name'],
                               c_type]
            query = f"INSERT INTO catalog_items ( \n" \
                    f"  catalog_item, \n" \
                    f"  catalog_name, \n" \
                    f"  class_name, \n" \
                    f"  infra_type) \n" \
                    f"VALUES ( \n" \
                    f"  %s, " \
                    f"  %s, " \
                    f"  %s, " \
                    f"  %s) \n " \
                    f"RETURNING id \n"

            if self.debug:
                self.logger.info(f"Inserting Catalog Item: \n{query}")

            try:
                # TODO: Add exception
                result = utils.execute_query(query, positional_args=positional_args, autocommit=True)

                if result['rowcount'] >= 1:
                    query_result = result['query_result'][0]
                    return query_result.get('id')
                else:
                    self.logger.error(f"Error inserting catalog {self.prov_data['catalog_item']}")
                    return -1
            except Exception:
                self.logger.error("Error inserting catalog item", stack_info=True)

        else:
            return catalog_id
