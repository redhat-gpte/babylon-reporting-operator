import utils
from datetime import datetime, timezone
import json


class Provisions(object):

    def __init__(self, logger, prov_data):
        self.debug = False
        self.logger = logger
        self.prov_data = prov_data
        self.user_data = self.prov_data.get('user', {})
        self.provision_uuid = self.prov_data.get('uuid')
        self.provision_guid = self.prov_data.get('guid', self.prov_data.get('babylon_guid'))

    def populate_purpose(self, purpose_name):

        if purpose_name is None:
            return {'id': None}

        category = 'Others'
        if purpose_name.startswith('Training'):
            category = 'Training'
        elif purpose_name.startswith('Development') or 'Content dev' in purpose_name:
            category = 'Development'
        elif 'Customer Activity' in purpose_name:
            category = 'Customer Activity'

        insert_fields = {
            'purpose': purpose_name,
            'category': category,
        }

        update_fields = {
            'purpose': purpose_name,
            'category': category,
        }

        query, positional_args = utils.create_sql_statement(insert_fields=insert_fields,
                                                            update_fields=update_fields,
                                                            table_name='purpose',
                                                            constraint='purpose_unique',
                                                            return_field='id')

        if self.debug:
            print(f"Query Insert: \n{query}")

        cur = utils.execute_query(query, positional_args=positional_args, autocommit=True)

        if cur['rowcount'] >= 1:
            query_result = cur['query_result'][0]
            return query_result
        else:
            return {'id': None}

    def populate_provisions(self):
        self.logger.info(f"Inserting Provision {self.provision_uuid} - {self.prov_data}")
        # if self.debug:
        #     print(json.dumps(self.prov_data, indent=2, default=str))
        catalog_id = self.prov_data.get('catalog_id', -1)
        if catalog_id == -1:
            self.logger.error("Error getting catalog_id")
            return False

        self.logger.info(f"Catalog ID: {catalog_id}")

        purpose = self.prov_data.get('purpose', 'Development')
        purpose_id = self.populate_purpose(purpose)
        purpose_id = purpose_id.get('id')

        user_db_info = self.prov_data.get('user_db', {})
        student_id = user_db_info.get('user_id')
        user_manager_id = user_db_info.get('manager_id')
        user_manager_chargeback_id  = user_db_info.get('manager_chargeback_id')
        user_cost_center = user_db_info.get('cost_center')

        current_state = self.prov_data.get('current_state')
        provision_result = 'installing'
        if current_state.startswith('provision-') and current_state != 'provision-pending':
            provision_result = self.prov_data.get('provision_result', provision_result)

        if provision_result == 'failed':
            provision_result = 'failure'
        elif provision_result == 'successful':
            provision_result = 'success'

        # TODO: Fix cloud ec2 to AWS and osp to openstack
        cloud = self.prov_data.get('cloud', 'unknown')

        # Dictionary of fields to be inserted
        insert_fields = {
            'student_id': student_id,
            'catalog_id': catalog_id,
            'provisioned_at': self.prov_data.get('provisioned_at', datetime.now(timezone.utc)),
            'datasource': self.prov_data.get('datasource', 'BABYLON'),
            'environment': self.prov_data.get('environment', 'DEV').upper(),
            'guid': self.prov_data.get('guid'),
            'uuid': self.provision_uuid,
            'babylon_guid': self.prov_data.get('babylon_guid'),
            'provision_result': provision_result,
            'account': self.prov_data.get('account', 'tests'),
            'cloud_region': self.prov_data.get('cloud_region'),
            'purpose': purpose,
            'cloud': cloud,
            'sandbox_name': self.prov_data.get('sandbox_name'),
            'workshop_users': self.prov_data.get('workshop_users', 1),
            'workload': self.prov_data.get('workload'),
            'provision_time': self.prov_data.get('provisiontime', 0),
            'deploy_interval': self.prov_data.get('deploy_interval'),
            'service_type': self.prov_data.get('servicetype', 'babylon'),
            'stack_retries': self.prov_data.get('stack_retries', 1),
            'opportunity': self.prov_data.get('opportunity'),
            'purpose_id': purpose_id,
            'tshirt_size': self.prov_data.get('tshirt_size'),
            'cost_center': user_cost_center,
            'student_geo': self.user_data.get('region', 'NA'),
            'manager_id': user_manager_id,
            'class_name': self.prov_data.get('class_name'),
            'chargeback_method': self.prov_data.get('chargeback_method', 'regional'),
            'manager_chargeback_id': user_manager_chargeback_id,
            'tower_job_id': self.prov_data.get('tower_job_id'),
            'tower_job_url': self.prov_data.get('tower_job_url'),
            'anarchy_governor': self.prov_data.get('anarchy_governor'),
            'anarchy_subject_name': self.prov_data.get('anarchy_subject_name'),
            'created_at': datetime.now(timezone.utc),
            'modified_at': datetime.now(timezone.utc),
            'last_state': current_state
        }

        using_cloud_forms = self.prov_data.get('using_cloud_forms', False)

        update_fields = {
            'student_id': student_id,
            'catalog_id': catalog_id,
            'provisioned_at': self.prov_data.get('provisioned_at', datetime.now(timezone.utc)),
            'datasource': self.prov_data.get('datasource', 'BABYLON'),
            'environment': self.prov_data.get('environment', 'DEV').upper(),
            # 'guid': self.prov_data.get('guid'),
            'uuid': self.provision_uuid,
            'babylon_guid': self.prov_data.get('babylon_guid'),
            'provision_result': provision_result,
            'account': self.prov_data.get('account', 'tests'),
            'cloud_region': self.prov_data.get('cloud_region'),
            'purpose': purpose,
            'cloud': cloud,
            'sandbox_name': self.prov_data.get('sandbox_name'),
            'workshop_users': self.prov_data.get('workshop_users', 1),
            'workload': self.prov_data.get('workload'),
            'provision_time': self.prov_data.get('provision_time', 0),
            'deploy_interval': self.prov_data.get('deploy_interval'),
            'service_type': self.prov_data.get('servicetype', 'babylon'),
            'stack_retries': self.prov_data.get('stack_retries', 1),
            'opportunity': self.prov_data.get('opportunity'),
            'purpose_id': purpose_id,
            'tshirt_size': self.prov_data.get('tshirt_size'),
            'cost_center': user_cost_center,
            'student_geo': self.user_data.get('region', 'NA'),
            'manager_id': user_manager_id,
            'class_name': self.prov_data.get('class_name'),
            'chargeback_method': self.prov_data.get('chargeback_method', 'regular'),
            'manager_chargeback_id': user_manager_chargeback_id,
            'tower_job_id': self.prov_data.get('tower_job_id'),
            'tower_job_url': self.prov_data.get('tower_job_url'),
            'anarchy_governor': self.prov_data.get('anarchy_governor'),
            'anarchy_subject_name': self.prov_data.get('anarchy_subject_name'),
            'modified_at': datetime.now(timezone.utc),
            'last_state': current_state
        }

        query, positional_args = utils.create_sql_statement(insert_fields=insert_fields,
                                                            update_fields=update_fields,
                                                            table_name='provisions',
                                                            constraint='provisions_pk',
                                                            return_field='uuid')

        if self.debug:
            print(f"Executing Query insert provisions {self.provision_uuid}: {query} - {positional_args}")

        cur = utils.execute_query(query, positional_args=positional_args, autocommit=True)

        if cur['rowcount'] >= 1:
            query_result = cur['query_result'][0]
            self.logger.info(f"Provision Database UUID: {query_result.get('uuid', None)}")

        return self.provision_uuid
