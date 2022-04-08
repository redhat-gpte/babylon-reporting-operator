import json
import utils
from corp_ldap import GPTELdap
from manager_chargeback import ManagerChargeback
from datetime import datetime, timezone


class Users(GPTELdap):
    def __init__(self, logger, prov_data):
        super().__init__(logger)
        self.debug = False
        self.logger = logger
        self.prov_data = prov_data
        self.user_data = self.prov_data.get('user', {})
        self.manager_data = self.user_data.get('manager', {})
        self.user_mail = self.user_data.get('mail', '').lower()
        self.manager_mail = None

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
        positional_args = [self.manager_mail]

        query = f"SELECT id FROM manager " \
                f"WHERE email = %s"

        result = utils.execute_query(query, positional_args=positional_args, autocommit=True)

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

        insert_fields = {
            'name': self.manager_data.get('cn'),
            'email': self.manager_data.get('mail'),
            'kerberos_id': self.manager_data.get('uid')

        }

        update_fields = {
            'name': self.manager_data.get('cn'),
            'email': self.manager_data.get('mail'),
            'kerberos_id': self.manager_data.get('uid')
        }

        query, positional_args = utils.create_sql_statement(insert_fields, update_fields, 'manager',
                                                            'manager_unique_email', 'id' )

        result = utils.execute_query(query, positional_args=positional_args, autocommit=True)

        if result['rowcount'] >= 1:
            return result['query_result'][0]
        else:
            return None

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
            manager_chargeback_id = None
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
        self.manager_mail = self.manager_data.get('mail')
        manager_name = self.manager_data.get('cn')
        manager_kerberos_id = self.manager_data.get('uid')
        user_kerberos_id = user_data.get('uid')
        user_title = user_data.get('title')
        user_cost_center = user_data.get('rhatCostCenter')
        user_geo = user_data.get('rhatGeo')

        if not self.manager_mail:
            manager_id = None
        else:
            manager_id = self.populate_manager().get('id')

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
        user_first_name = self.user_data.get('givenName', 'default').capitalize().strip()
        user_last_name = self.user_data.get('sn', 'default').capitalize().strip()
        user_full_name = f"{user_first_name} {user_last_name}"

        self.user_data['partner'] = 'partner'
        gpte_user = 'Only Regular Users'

        if '@redhat.com' in self.user_mail:
            self.user_data['partner'] = 'redhat'
            self.search_internal_user()
            company_id = 16736
        elif 'poolboy' in self.user_mail:
            self.user_data['partner'] = 'redhat'
            self.user_data['cost_center'] = 99999
            gpte_user = 'Poolboy'
            company_id = 16736
        elif 'ibm.com' in self.user_mail:
            self.user_data['partner'] = 'IBM'
            self.user_data['cost_center'] = None
            self.user_data['kerberos_id'] = None
            self.manager_data['cn'] = None
            self.manager_data['mail'] = None
            company_id = 13716
        else:
            self.user_data['kerberos_id'] = None
            self.manager_data['cn'] = None
            self.manager_data['mail'] = None
            company_id = 10000

        user_geo = self.user_data.get('rhatGeo', self.user_data.get('region', 'NA'))

        # I have to quote and unquote when we have values and using default values when we don't have
        # self.user_data = self.user_data
        self.manager_data = self.manager_data
        self.user_data['user_id'] = self.user_data.get('user_id')

        # Only GPTE Exclusions
        pfe_managers = ['sborenst@redhat.com', 'oczernin@redhat.com',
                        'nalentor@redhat.com', 'jenkins.sfo01@redhat.com',
                        'jenkins.sfo01@gmail.com', 'brezhnev@redhat.com'
                        ]

        if self.manager_mail in pfe_managers or self.user_mail in pfe_managers:
            gpte_user = 'Only GPTE Exclusions'

        print(f"PFE USER: {gpte_user}")
        print(f"PFE Manager Email: {self.manager_mail}")
        print(f"PFE USER Manager: '{self.manager_data.get('mail', '')}'")

        insert_fields = {
            'company_id': company_id,
            'username': self.user_data.get('uid'),
            'email': self.user_mail,
            'full_name': user_full_name,
            'geo': user_geo,
            'partner': self.user_data.get('partner'),
            'cost_center': self.user_data.get('cost_center'),
            'created_at': datetime.now(timezone.utc),
            'kerberos_id': self.user_data.get('kerberos_id'),
            'manager': self.manager_data.get('cn'),
            'manager_email': self.manager_data.get('mail'),
            'title': self.user_data.get('title'),
            'first_name': user_first_name,
            'last_name': user_last_name,
            'gpte_user': gpte_user,
            'check_headcount': self.user_data.get('check_headcount', True)
        }

        update_fields = {
            'company_id': company_id,
            'username': self.user_data.get('uid'),
            'email': self.user_mail,
            'full_name': user_full_name,
            'geo': user_geo,
            'partner': self.user_data.get('partner'),
            'cost_center': self.user_data.get('cost_center'),
            'kerberos_id': self.user_data.get('kerberos_id'),
            'manager': self.manager_data.get('cn'),
            'manager_email': self.manager_data.get('mail'),
            'title': self.user_data.get('title'),
            'first_name': user_first_name,
            'last_name': user_last_name,
            'gpte_user': gpte_user,
            'check_headcount': self.user_data.get('check_headcount', True)
        }

        query, positional_args = utils.create_sql_statement(insert_fields=insert_fields,
                                                            update_fields=update_fields,
                                                            table_name='students',
                                                            constraint='students_unique_email',
                                                            return_field='id,check_headcount')

        if self.debug:
            print(f"Query Insert: \n{query}", positional_args)

        cur = utils.execute_query(query, positional_args=positional_args, autocommit=True)
        if cur['rowcount'] >= 1:
            query_result = cur['query_result'][0]
            self.user_data['user_id'] = query_result.get('id')
            self.user_data['check_headcount'] = query_result.get('check_headcount', True)

        results = {
            'user_id': self.user_data['user_id'],
            'manager_chargeback_id': self.user_data['manager_chargeback_id'],
            'manager_id': self.user_data['manager'].get('manager_id'),
            'cost_center': self.user_data.get('cost_center')
        }
        return results
