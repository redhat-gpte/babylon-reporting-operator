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

    def populate_purpose(self, purpose_name):
        query = f"SELECT id FROM purpose WHERE purpose = '{purpose_name}' LIMIT 1;"
        if self.debug:
            print(f"Searching purpose: {query}")
            self.logger.debug(f"Searching purpose: {query}")

        result = utils.execute_query(query)

        if result['rowcount'] >= 1:
            query_result = result['query_result'][0]
            return query_result

        else:
            category = 'Others'
            if purpose_name.startswith('Training'):
                category = 'Training'
            elif purpose_name.startswith('Development') or 'Content dev' in purpose_name:
                category = 'Development'
            elif 'Customer Activity' in purpose_name:
                category = 'Customer Activity'
            query_insert = f"INSERT INTO purpose (purpose, category) \n" \
                           f"VALUES ('{purpose_name}', '{category}') \n" \
                           f"RETURNING id;"
            if self.debug:
                print(f"New purpose: {query_insert}")
                self.logger.debug(f"New purpose: {query_insert}")

            result = utils.execute_query(query_insert, autocommit=True)

            if result['rowcount'] >= 1:
                query_result = result['query_result'][0]
                return query_result
            else:
                return {'id': 'default'}

    def update_provisions(self):
        self.logger.info(f"Updating provision {self.provision_uuid} - "
                         f"Current State: {self.prov_data.get('current_state')}")
        user_db_info = self.prov_data.get('user_db', {})
        user_db_id = user_db_info.get('user_id', None)
        user_manager_id = user_db_info.get('manager_id')
        user_manager_chargeback_id  = user_db_info.get('manager_chargeback_id')
        user_cost_center = user_db_info.get('cost_center', '441')

        query = f"UPDATE provisions SET \n" \
                f"  student_id = {user_db_id}, \n" \
                f"  catalog_id = {self.prov_data.get('catalog_id', -1)}, \n" \
                f"  guid = {utils.parse_null_value(self.prov_data.get('guid'))}, \n" \
                f"  cost_center = {user_cost_center}, \n" \
                f"  student_geo = '{self.user_data.get('region')}', \n" \
                f"  manager_id = {user_manager_id}, \n" \
                f"  manager_chargeback_id = {user_manager_chargeback_id} \n" \
                f"WHERE \n" \
                f"  uuid = '{self.provision_uuid}' \n" \
                f"RETURNING uuid;"
        self.logger.info(f"Updating Provistion {self.provision_uuid}")

        if self.debug:
            print(f"Query: {query}")

        cur = utils.execute_query(query, autocommit=True)

        if cur['rowcount'] >= 1:
            query_result = cur['query_result'][0]
            self.logger.info(f"Updating Provision Database UUID: {query_result.get('uuid', None)}")

    def populate_provisions(self):

        # If provision UUID already exists, we have to return because UUID is primary key
        if self.check_provision_exists() != -1:
            self.update_provisions()
            self.logger.info(f"Provision {self.provision_uuid} already exists. Skipping")
            return self.provision_uuid

        self.logger.info(f"Inserting Provision {self.provision_uuid}")

        catalog_id = self.prov_data.get('catalog_id', -1)
        if catalog_id == -1:
            self.logger.error("Error getting catalog_id")
            return False

        self.logger.info(f"Catalog ID: {catalog_id}")

        purpose = self.prov_data.get('purpose', 'Development')
        purpose_id = self.populate_purpose(purpose)
        purpose_id = purpose_id.get('id')

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
                f"  guid, \n" \
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
                f"  manager_chargeback_id," \
                f"  tower_job_id \n" \
                f") \n" \
                f"VALUES ( \n" \
                f"  '{self.prov_data.get('provisioned_at', provisioned_at)}', \n" \
                f"  {user_db_id}, \n" \
                f"  {catalog_id}, \n" \
                f"  {self.prov_data.get('workshop_users', 'default')}, \n" \
                f"  {self.prov_data.get('workload', 'default')}, \n" \
                f"  '{self.prov_data.get('servicetype', 'babylon')}', \n" \
                f"  {utils.parse_null_value(self.prov_data.get('guid'))}, \n" \
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
                f"  '{purpose}', \n" \
                f"  '{cloud}', \n" \
                f"  {self.prov_data.get('stack_retries', 1)}, \n" \
                f"  {purpose_id}, \n" \
                f"  {self.prov_data.get('tshirt_size', 'default')}, \n" \
                f"  {user_cost_center}, \n" \
                f"  '{self.user_data.get('region')}', \n" \
                f"  {user_manager_id}, \n" \
                f"  '{self.prov_data.get('class_name', 'NULL')}', \n" \
                f"  {self.prov_data.get('chargeback_method', utils.parse_null_value('NULL'))}, \n" \
                f"  {user_manager_chargeback_id}, \n" \
                f"  {utils.parse_null_value(self.prov_data.get('tower_job_id'))} \n) RETURNING uuid;"

        if self.debug:
            print(f"Executing Query insert provisions: {query}")

        cur = utils.execute_query(query, autocommit=True)

        if cur['rowcount'] >= 1:
            query_result = cur['query_result'][0]
            self.logger.info(f"Provision Database UUID: {query_result.get('uuid', None)}")

        return self.provision_uuid


