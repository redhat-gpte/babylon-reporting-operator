import utils
from datetime import datetime


class Provisions(object):

    def __init__(self, logger, prov_data):
        self.debug = False
        self.logger = logger
        self.prov_data = prov_data
        self.user_data = self.prov_data.get('user')
        self.provision_uuid = self.prov_data.get('uuid')
        self.provision_guid = self.prov_data.get('guid', self.prov_data.get('babylon_guid'))

    def check_provision_exists(self):
        query = f"SELECT uuid from provisions \n" \
                f"WHERE uuid = '{self.provision_uuid}'"
        result = utils.execute_query(query)
        if result['rowcount'] >= 1:
            query_result = result['query_result'][0]
            return query_result
        else:
            return -1

    # TODO: How to get purpose from Babylon
    def populate_purpose(self, purpose_name):
        query = f"SELECT * FROM purpose WHERE purpose = {purpose_name}"
        if self.debug:
            print(f"Finding purpose: {query}")
            self.logger.debug(f"Finding purpose: {query}")
        result = utils.execute_query(query)

        if result:
            return result['id']
        else:
            category = 'Others'
            if purpose_name.startswith('Training'):
                category = 'Training'
            elif purpose_name.startswith('Development') or 'Content dev' in purpose_name:
                category = 'Development'
            elif 'Customer Activity' in purpose_name:
                category = 'Customer Activity'
            query_insert = f"INSERT INTO purpose (purpose, category) VALUES ({purpose_name}, '{category}') RETURNING id;"
            if self.debug:
                print(f"New purpose: {query}")
                self.logger.debug(f"New purpose: {query}")

            result = utils.execute_query(query)
            if result['rowcount'] >= 1:
                query_result = result['query_result'][0]
                return query_result
            else:
                return -1

    def populate_provisions(self):

        # If provision UUID already exists, we have to return because UUID is primary key
        if self.check_provision_exists() != -1:
            self.logger.info(f"Provision {self.provision_uuid} already exists. Skipping")
            return self.provision_uuid

        self.logger.info(f"Inserting Provision {self.provision_uuid}")

        catalog_id = self.prov_data.get('catalog_id', -1)
        if catalog_id == -1:
            self.logger.error("Error getting catalog_id")
            return False

        self.logger.info(f"Catalog ID: {catalog_id}")

        # TODO: Update purpose
        purpose_id = 4

        user_db_info = self.prov_data.get('user_db', {})
        user_db_id = user_db_info.get('user_id')
        user_manager_id = user_db_info.get('manager_id')
        user_manager_chargeback_id  = user_db_info.get('manager_chargeback_id')
        user_cost_center = user_db_info.get('cost_center')

        current_state = self.prov_data.get('current_state')
        provision_results = 'success'
        if current_state.startswith('provision-') and current_state != 'provision-pending':
            provision_results = current_state.replace('provision-', '')

        # TODO: Fix provision results
        if provision_results == 'failed':
            provision_results = 'failure'

        # TODO: Fix cloud ec2 to AWS and osp to openstack
        cloud = self.prov_data.get('cloud', 'unknown')

        provisioned_at = datetime.utcnow()
        query = f"INSERT INTO provisions (\n" \
                f"  provisioned_at, \n" \
                f"  student_id, \n" \
                f"  catalog_id, \n" \
                f"  workshop_users, \n" \
                f"  workload, \n" \
                f"  service_type, \n" \
                f"  -- guid, \n" \
                f"  uuid, \n" \
                f"  opportunity, \n" \
                f"  account, \n" \
                f"  sandbox_name, \n" \
                f"  provision_result, \n" \
                f"  datasource, \n" \
                f"  environment, \n" \
                f"  provision_time, \n" \
                f"  cloud_region, \n" \
                f"  babylon_guid, \n" \
                f"  purpose, \n" \
                f"  cloud, \n" \
                f"  stack_retries, \n" \
                f"  purpose_id, \n" \
                f"  tshirt_size, \n" \
                f"  cost_center, \n" \
                f"  student_geo, \n" \
                f"  manager_id, \n" \
                f"  class_name, \n" \
                f"  chargeback_method, \n" \
                f"  manager_chargeback_id \n" \
                f") \n" \
                f"VALUES ( \n" \
                f"  '{self.prov_data.get('provisioned_at', provisioned_at)}', \n" \
                f"  {user_db_id}, \n" \
                f"  {catalog_id}, \n" \
                f"  {self.prov_data.get('workshop_users', 'default')}, \n" \
                f"  {self.prov_data.get('workload', 'default')}, \n" \
                f"  '{self.prov_data.get('servicetype', 'babylon')}', \n" \
                f"  -- '{self.provision_guid}', \n" \
                f"  '{self.provision_uuid}', \n" \
                f"  {self.prov_data.get('opportunity', 'default')}, \n" \
                f"  '{self.prov_data.get('account', 'tests')}', \n" \
                f"  {utils.parse_null_value(self.prov_data.get('sandbox_name', 'default'))}, \n" \
                f"  '{provision_results}', \n" \
                f"  '{self.prov_data.get('datasource', 'RHDPS')}', \n" \
                f"  '{self.prov_data.get('environment', 'DEV').upper()}', \n" \
                f"  {self.prov_data.get('provisiontime', 0)}, \n" \
                f"  {utils.parse_null_value(self.prov_data.get('cloud_region', 'default'))}, \n" \
                f"  '{self.prov_data.get('babylon_guid', utils.parse_null_value('NULL'))}', \n" \
                f"  {self.prov_data.get('purpose', utils.parse_null_value('default'))}, \n" \
                f"  '{cloud}', \n" \
                f"  {self.prov_data.get('stack_retries', 1)}, \n" \
                f"  {purpose_id}, \n" \
                f"  {self.prov_data.get('tshirt_size', 'default')}, \n" \
                f"  {user_cost_center}, \n" \
                f"  '{self.user_data.get('region')}', \n" \
                f"  {user_manager_id}, \n" \
                f"  '{self.prov_data.get('class_name', 'NULL')}', \n" \
                f"  {self.prov_data.get('chargeback_method', utils.parse_null_value('NULL'))}, \n" \
                f"  {user_manager_chargeback_id} \n) RETURNING uuid;"

        if self.debug:
            print(f"Executing Query insert provisions: {query}")

        cur = utils.execute_query(query, autocommit=True)

        if cur['rowcount'] >= 1:
            query_result = cur['query_result'][0]
            self.logger.info(f"Provision Database UUID: {query_result.get('uuid', None)}")

        return self.provision_uuid

    def provision_complete(self):
        self.logger.info("Provision Completed")

    def destroy_complete(self):
        self.logger.info("Destroy Complete")

    def destroy(self):
        self.logger.info("Destroy Complete")

    def start_complete(self):
        self.logger.info("Start Complete")

    def stop_complete(self):
        self.logger.info("Stop Complete")


