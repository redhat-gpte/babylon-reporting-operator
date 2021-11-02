import json
import utils
from corp_ldap import GPTELdap
from manager_chargeback import ManagerChargeback


class Users(GPTELdap):
    def __init__(self, logger, prov_data):
        super().__init__(logger)
        self.debug = False
        self.logger = logger
        self.prov_data = prov_data
        self.user_data = self.prov_data.get('user', {})
        self.manager_data = self.user_data.get('manager', {})
        self.user_mail = self.user_data.get('mail', '').lower()
        self.manager_mail = 'default'

    def check_user_exists(self):
        """
        Check if students email exists in the database and returns ID and check_headcount.
        It's ordering by created_at to use the first user created.
        :return: a dictionary
        dict(
            id,
            check_headcount,
        )
        """
        query = f"SELECT id, check_headcount FROM students " \
                f"WHERE email = '{self.user_mail}' ORDER BY created_at"
        result = utils.execute_query(query)
        if result['rowcount'] >= 1:
            query_result = result['query_result'][0]
            return query_result
        else:
            return -1

    def get_company_id(self):
        """
        This method is just for compatibility with costs reports
        :return: company_id
        """
        if '@' in self.user_mail and '@redhat.com' in self.user_mail:
            return 16736
        if '@' in self.user_mail and 'ibm.com' in self.user_mail:
            return 13716
        else:
            return 10000

    def get_manager_chargeback(self):
        """
        This method search all service manager chargeback from service_chargeback table

        :return: a list of manager chargeback emails and ID
        """
        manager = ManagerChargeback(self.logger)
        return manager.list_manager()

    def check_manager_exists(self):
        """
        Check if manager already exists in the managers table
        :return: manager_id or -1 if the manager doesn't exists yet
        """
        query = f"SELECT id FROM manager " \
                f"WHERE email = {utils.parse_null_value(self.manager_mail)}"
        result = utils.execute_query(query)
        if result['rowcount'] >= 1:
            query_result = result['query_result'][0]
            return query_result
        else:
            return -1

    def populate_manager(self):
        """
        Insert manager into managers table
        :return: manager_id
        """
        manager_id = self.check_manager_exists()
        # If manager doesn't exists insert into
        if manager_id == -1:
            query = f"INSERT INTO manager (name, email, kerberos_id) \n" \
                    f"VALUES ( \n" \
                    f"  '{self.manager_data['cn']}', \n" \
                    f"  '{self.manager_data['mail']}', \n" \
                    f"  '{self.manager_data['uid']}') RETURNING id;"
            result = utils.execute_query(query, autocommit=True)
            if result['rowcount'] >= 1:
                manager_id = result['query_result'][0]
            else:
                return -1
        else:
            # TODO: Update Manager????
            pass
        return manager_id

    def search_internal_user(self):
        """
        This is method is used only when we have student.email like @redhat.com search in RH CORP LDAP
        user's information and manager chargeback

        :return:
        """
        user_first_name = self.user_data.get('givenName').capitalize().strip()
        user_last_name = self.user_data.get('sn').capitalize().strip()
        generic_email = utils.generic_email(self.user_mail)

        if self.debug:
            print(f"search_internal_user: \n"
                  f"  user_first_name: {user_first_name} \n"
                  f"  user_last_name: {user_last_name} \n"
                  f"  generic_email: {generic_email} \n"
                  f"  user_mail: {self.user_mail} \n"
                  f"  ")

        # Getting manager to be charged back when check_headcount is true
        # Get a list of manager to be charged
        manager_list = self.get_manager_chargeback()

        # Serach in LDAP if user's manager is in the list of manager_list to be charged
        chargeback_manager_mail = self.ldap_user_headcount(generic_email, manager_list)

        if isinstance(chargeback_manager_mail, dict) or \
                chargeback_manager_mail == 'gpte@redhat.com':
            manager_chargeback_id = 'default'
        else:
            manager_chargeback_id = manager_list[chargeback_manager_mail]

        if self.debug:
            print(f"search_internal_user: \n"
                  f" chargeback_manager_mail: {chargeback_manager_mail} \n"
                  f" manager_chargeback_id: {manager_chargeback_id} \n"
                  f"")

        user_data = self.ldap_search_user(generic_email)

        if self.debug:
            print("search_internal_user: \n"
                  f"  user_data: {json.dumps(user_data, indent=2)} \n"
                  f"")

        # If can't find the user using email we trying to get the user using
        # user_first_name.lower()[:1]+user_last_name.lower()[0:8]+'@redhat.com'
        if len(user_data) == 0:
            n_email = user_first_name.lower()[:1] + user_last_name.lower()[0:8] + '@redhat.com'
            self.logger.info(f"Search LDAP user using First Name and Last {generic_email} - for email {n_email}")
            user_data = self.ldap_search_user(n_email)

        self.manager_data = user_data.get('manager', {})
        self.manager_mail = self.manager_data.get('mail', 'default')
        manager_name = self.manager_data.get('cn', 'default').replace("'", '')
        manager_kerberos_id = self.manager_data.get('uid', 'default')
        user_kerberos_id = user_data.get('uid', 'default')
        user_title = user_data.get('title', 'default')
        user_cost_center = user_data.get('rhatCostCenter', 'default')
        user_geo = user_data.get('rhatGeo', 'default')

        if self.manager_mail == 'default':
            manager_id = 'default'
        else:
            manager_id = self.populate_manager().get('id', 'default')

        if self.debug:
            print("search_internal_user: \n"
                  f"  manager_data: {json.dumps(self.manager_data, indent=2)} \n"
                  f"  manager_manager_id: {manager_id} \n"
                  f"  manager_name: {manager_name} \n"
                  f"  manager_mail: {self.manager_mail} \n"
                  f"  manager_kerberos_id: {manager_kerberos_id} \n"
                  f"  user_kerberos_id: {user_kerberos_id} \n"
                  f"  user_title: {user_title} \n"
                  f"  user_cost_center: {user_cost_center} \n"
                  f"  user_geo: {user_geo} \n"
                  f"")

        result = {'cost_center': user_cost_center,
                  'region': user_geo,
                  'title': user_title,
                  'kerberos_id': user_kerberos_id,
                  'manager': {
                      'name': manager_name,
                      'email': self.manager_mail,
                      'kerberos_id': manager_kerberos_id,
                      'manager_id': manager_id
                  },
                  'manager_chargeback_id': manager_chargeback_id
                  }
        if self.debug:
            print("search_internal_user: results \n"
                  f"{json.dumps(result, indent=2)}")

        self.user_data.update(result)

    def populate_users(self):
        """
        This method is responsable to keep the students table updated or adding new users.
        If student email is like @redhat.com, we have to:
          1) Search user in RH Corp LDAP to get cost_center, region and direct manager information
          2) Search chargeback manager in RH CORP LDAP
          3) Populate manager table

        Returning a dictionary to be used in Provisions Table

        :return: a dictionary with
            { user_id,
              manager_chargeback_id,
              manager_id,
              cost_center
            }
        """
        # TODO: How to get User's geo from IPA
        user_first_name = self.user_data.get('givenName', 'default').capitalize().strip()
        user_last_name = self.user_data.get('sn', 'default').capitalize().strip()
        user_full_name = f"{user_first_name} {user_last_name}"

        self.user_data['partner'] = 'partner'
        if '@redhat.com' in self.user_mail:
            self.user_data['partner'] = 'redhat'
            self.search_internal_user()
        elif 'ibm.com' in self.user_mail:
            self.user_data['partner'] = 'IBM'
            self.user_data['cost_center'] = None
            self.user_data['kerberos_id'] = None
            self.manager_data['cn'] = None
            self.manager_data['mail'] = None
        else:
            self.user_data['kerberos_id'] = None
            self.manager_data['cn'] = None
            self.manager_data['mail'] = None

        user_results = self.check_user_exists()
        if isinstance(user_results, dict):
            self.user_data['user_id'] = user_results.get('id', -1)
            self.user_data['check_headcount'] = user_results.get('check_headcount', True)
        else:
            self.user_data['user_id'] = -1
            self.user_data['check_headcount'] = True

        company_id = self.get_company_id()

        user_geo = utils.parse_null_value(self.user_data.get('rhatGeo',
                                                             self.user_data.get('region', 'default')))

        # I have to quote and unquote when we have values and using default values when we don't have
        self.user_data = utils.parse_dict_null_value(self.user_data)
        self.manager_data = utils.parse_dict_null_value(self.manager_data)
        self.user_data['user_id'] = int(self.user_data['user_id'].replace("'", ''))

        # if students exists, update few information in the database based in RH LDAP or IPA
        if self.user_data['user_id'] >= 1:
            # TODO: Student exists, update??
            user_title = self.user_data.get('title')
            user_manager = self.manager_data.get('cn', 'default')
            user_manager_mail = self.manager_data.get('mail', 'default')
            query = f"UPDATE students SET \n" \
                    f"  geo = {user_geo}, \n" \
                    f"  partner = {self.user_data.get('partner')}, \n" \
                    f"  cost_center = {self.user_data.get('cost_center')}, \n" \
                    f"  manager = {user_manager}, \n" \
                    f"  manager_email = {user_manager_mail}, \n" \
                    f"  title = {user_title} \n" \
                    f"WHERE id = {self.user_data['user_id']} \n" \
                    f"RETURNING id;"

            if self.debug:
                print(f"Querying Updating User:\n{query}")

            cur = utils.execute_query(query, autocommit=True)

        elif self.user_data['user_id'] == -1:
            query = f"INSERT INTO students ( \n" \
                    f"  company_id, \n" \
                    f"  username, \n" \
                    f"  email, \n" \
                    f"  full_name, \n" \
                    f"  geo, \n" \
                    f"  partner, \n" \
                    f"  cost_center, \n" \
                    f"  created_at, \n" \
                    f"  kerberos_id, \n" \
                    f"  manager, \n" \
                    f"  manager_email, \n" \
                    f"  title, \n" \
                    f"  first_name, \n" \
                    f"  last_name, \n" \
                    f"  check_headcount \n) \n" \
                    f"VALUES ( \n" \
                    f"  {company_id}, \n" \
                    f"  {self.user_data['uid']}, \n" \
                    f"  '{self.user_mail}', \n" \
                    f"  '{user_full_name}', \n" \
                    f"  {user_geo}, \n" \
                    f"  {self.user_data.get('partner')}, \n" \
                    f"  {self.user_data.get('cost_center')}, \n" \
                    f"  NOW(), \n" \
                    f"  {self.user_data.get('kerberos_id')}, \n" \
                    f"  {self.manager_data.get('cn', 'default')}, \n" \
                    f"  {self.manager_data.get('mail', 'default')}, \n" \
                    f"  {self.user_data.get('title')}, \n" \
                    f"  '{user_first_name}', \n" \
                    f"  '{user_last_name}', \n" \
                    f"  {self.user_data.get('check_headcount')} \n" \
                    f") RETURNING id;"

            if self.debug:
                print(f"Query Insert: \n{query}")

            cur = utils.execute_query(query, autocommit=True)
            if cur['rowcount'] >= 1:
                query_result = cur['query_result'][0]
                self.user_data['user_id'] = query_result['id']

        results = {
            'user_id': self.user_data['user_id'],
            'manager_chargeback_id': self.user_data['manager_chargeback_id'],
            'manager_id': self.user_data['manager'].get('manager_id', 'default'),
            'cost_center': self.user_data.get('cost_center')
        }
        return results
