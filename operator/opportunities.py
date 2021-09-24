from simple_salesforce import Salesforce
import requests
import os
import utils
from retrying import retry


class SalesForce(object):

    def __init__(self, logger):
        # TODO: This is a test, remove it before commit
        self.sf_info = utils.get_secret_data("gpte-sf-secrets")
        self.sf_cert_key_file = "/etc/sfdc.pem"
        self.sf_conn = None
        self.debug = False
        self.logger = logger
        if not os.path.exists(self.sf_cert_key_file):
            with open(os.open(self.sf_cert_key_file, os.O_CREAT | os.O_WRONLY, 0o777), 'w') as fh:
                fh.write(self.sf_info['sf_cert_key'])

    # Wait 2^x * 500 milliseconds between each retry, up to 5 seconds, then 5 seconds afterwards and 3 attempts
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=500, wait_exponential_max=5000)
    def sf_connect(self):
        try:
            session = requests.Session()
            sf = Salesforce(instance=self.sf_info['sf_host'],
                            consumer_key=self.sf_info['sf_consumer_key'],
                            privatekey_file=self.sf_cert_key_file,
                            username=self.sf_info['sf_username'],
                            client_id="GPTE Provision", session=session)
            return sf
        except Exception as e:
            self.logger.error("Error connecting to SalesForce", stack_info=True)
            raise Exception(f"Failed to connect {e}")

    def execute_sf_query(self, query):
        if self.sf_conn is None:
            self.sf_conn = self.sf_connect()
        try:
            results = self.sf_conn.query(query)
            return results
        except Exception as e:
            self.logger.error(f"Error executing SalesForce {query} - {e}")
            return -1

    def get_sf_owner(self, owner_id):
        owner_data = {}
        query = f"SELECT Name, Email, Title FROM User WHERE Id = '{owner_id}'"
        owner_info = self.execute_sf_query(query)
        if owner_info == -1:
            owner_data.update({'OwnerName': None})
            owner_data.update({'OwnerEmail': None})
            owner_data.update({'OwnerTitle': None})
        else:
            for i in owner_info['records']:
                owner_data.update({'OwnerName': i['Name']})
                owner_data.update({'OwnerEmail': i['Email']})
                owner_data.update({'OwnerTitle': i['Title']})
        return owner_data

    def get_sf_account(self, account_id):
        acc_data = {}
        query = f"SELECT Name FROM Account WHERE Id = '{account_id}'"
        acc_info = self.execute_sf_query(query)
        if acc_info == -1:
            acc_data.update({'AccountName': None})
        else:
            for i in acc_info['records']:
                acc_data.update({'AccountName': i['Name']})
        return acc_data

    def get_sf_opportunity(self, opp_id):
        opp_data = {}
        opp_query = f"SELECT Id, Name, AccountId, OwnerId, Type, IsClosed, CloseDate, StageName, Amount, " \
                    f"ExpectedRevenue, OpportunityNumber__c FROM Opportunity WHERE Id =  {opp_id}"
        # Trying to query by a opportunity ID, but we don't know if exists, if raise SalesforceMalformedRequest,
        # the opportunity number is invalid
        if self.debug:
            print(f"Query SalesForce Opportunity by ID: \n{opp_query}")

        opp_info = self.execute_sf_query(opp_query)
        if opp_info == -1:
            opp_data = {}
        else:
            for i in opp_info['records']:
                if 'attributes' in i:
                    del i['attributes']

                opp_data.update(i)
                account_id = i['AccountId']
                owner_id = i['OwnerId']
                opp_data.update(self.get_sf_account(account_id))
                opp_data.update(self.get_sf_owner(owner_id))
        return opp_data

    def get_sf_opportunity_by_number(self, opp_id):
        opp_data = {}
        opp_query = f"SELECT Id, Name, AccountId, OwnerId, Type, IsClosed, CloseDate, StageName, Amount, " \
                    f"ExpectedRevenue, OpportunityNumber__c FROM Opportunity " \
                    f"WHERE OpportunityNumber__c =  {opp_id}"
        # Trying to query by a opportunity number, but we don't know if exists, if raise SalesforceMalformedRequest,
        # the opportunity number is invalid
        if self.debug:
            print(f"Query SalesForce Opportunity by Number: \n{opp_query}")
        opp_info = self.execute_sf_query(opp_query)
        if opp_info == -1:
            opp_data = {}
        else:
            for i in opp_info['records']:
                if 'attributes' in i:
                    del i['attributes']

                opp_data.update(i)
                account_id = i['AccountId']
                owner_id = i['OwnerId']
                opp_data.update(self.get_sf_account(account_id))
                opp_data.update(self.get_sf_owner(owner_id))

        return opp_data


class Opportunities(SalesForce):

    def __init__(self, logger, prov_data):
        super().__init__(logger)
        self.logger = logger
        self.prov_data = prov_data
        self.opp_id = utils.parse_null_value(self.prov_data.get('opportunity', None))

    def check_opportunity_exists(self):
        query = f"SELECT * FROM opportunities \n" \
                f"WHERE opportunity_id = {self.opp_id} or number = {self.opp_id};"
        result = utils.execute_query(query)
        if result['rowcount'] >= 1:
            query_result = result['query_result'][0]
            return query_result
        else:
            return -1

    def populate_opportunities(self):

        # If we don't have opportunity just return
        if self.opp_id == 'default' or \
                self.opp_id is None or \
                self.opp_id == 'NULL':
            return

        opp_by_number = False
        self.logger.info(f"Trying to get opportunity by Number: {self.opp_id}")

        opp_info = self.get_sf_opportunity_by_number(self.opp_id)

        if len(opp_info) == 0:
            self.logger.warning(f"Can't find opportunity {self.opp_id} by Number. Trying to get by ID")
            opp_info = self.get_sf_opportunity(self.opp_id)
        else:
            opp_by_number = True

        if len(opp_info) == 0:
            self.logger.warning(f"Can't find opportunity {self.opp_id} by ID")
            return False

        account_id = utils.parse_null_value(opp_info['AccountId'])
        account_name = utils.parse_null_value(opp_info['AccountName'].replace("'", " "))
        opp_amount = utils.parse_null_value(opp_info['Amount'])
        closed_date = utils.parse_null_value(opp_info['CloseDate'])
        revenue = utils.parse_null_value(opp_info['ExpectedRevenue'])
        opp_closed = utils.parse_null_value(opp_info['IsClosed'])
        opp_name = utils.parse_null_value(opp_info['Name'].replace("'", " "))
        owner_email = utils.parse_null_value(opp_info['OwnerEmail'])
        owner_id = utils.parse_null_value(opp_info['OwnerId'])
        owner_name = utils.parse_null_value(opp_info['OwnerName'])
        owner_title = utils.parse_null_value(opp_info['OwnerTitle'])
        stage = utils.parse_null_value(opp_info['StageName'])
        opp_type = utils.parse_null_value(opp_info['Type'])
        opportunity_id = utils.parse_null_value(opp_info['Id'])
        opp_number = utils.parse_null_value(opp_info['OpportunityNumber__c'])

        opp_results = self.check_opportunity_exists()

        # If opportunity not exists we have to insert into database
        opp_db_number = -1
        if isinstance(opp_results, dict):
            opp_db_number = opp_results.get('number', -1)

        if opp_db_number == -1:
            self.logger.info(f"Populate opportunity {self.opp_id}")
            query = f"INSERT INTO opportunities ( \n" \
                    f"  account_id, \n" \
                    f"  account_name, \n" \
                    f"  amount, \n" \
                    f"  closed_at, \n" \
                    f"  expected_revenue, \n" \
                    f"  opportunity_id, \n" \
                    f"  is_closed, \n" \
                    f"  opportunity_name, \n" \
                    f"  owner_email, \n" \
                    f"  owner_id, \n" \
                    f"  owner_name, \n" \
                    f"  owner_title, \n" \
                    f"  stage, \n" \
                    f"  type, \n" \
                    f"  number \n" \
                    f") \n" \
                    f"VALUES ( \n" \
                    f"  {account_id}, \n" \
                    f"  {account_name}, \n" \
                    f"  {opp_amount}, \n" \
                    f"  {closed_date}, \n" \
                    f"  {revenue}, \n" \
                    f"  {opportunity_id}, \n" \
                    f"  {opp_closed}, \n" \
                    f"  {opp_name}, \n" \
                    f"  {owner_email}, \n" \
                    f"  {owner_id}, \n" \
                    f"  {owner_name}, \n" \
                    f"  {owner_title}, \n" \
                    f"  {stage}, \n" \
                    f"  {opp_type}, \n" \
                    f"  {opp_number} \n" \
                    f") \n"
        else:
            self.logger.info(f"Updating opportunity {self.opp_id}")
            query = f"UPDATE opportunities SET \n" \
                    f"  opportunity_id = {opportunity_id}, \n" \
                    f"  account_id = {account_id}, \n" \
                    f"  account_name = {account_name}, \n" \
                    f"  amount = {opp_amount}, \n" \
                    f"  closed_at = {closed_date}, \n" \
                    f"  expected_revenue = {revenue}, \n" \
                    f"  is_closed = {opp_closed}, \n" \
                    f"  opportunity_name = {opp_name}, \n" \
                    f"  owner_email = {owner_email}, \n" \
                    f"  owner_id = {owner_id}, \n" \
                    f"  owner_name = {owner_name}, \n" \
                    f"  owner_title = {owner_title}, \n" \
                    f"  stage = {stage}, \n" \
                    f"  type = {opp_type}, \n" \
                    f"  number = {opp_number}, \n" \
                    f"  update_at = NOW() \n"
            if opp_by_number:
                query += f"WHERE number = {opp_number} \n"
            else:
                query += f"WHERE opportunity_id = {self.opp_id} \n"

        query += "RETURNING id;"
        if self.debug:
            print(f"Query Opportunity: \n{query}")

        cur = utils.execute_query(query, autocommit=True)

        if cur['rowcount'] >= 1:
            query_result = cur['query_result'][0]
            self.logger.info(f"Opportunity Database ID: {query_result.get('id')}")
        return True
