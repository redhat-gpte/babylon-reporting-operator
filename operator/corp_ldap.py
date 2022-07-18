import ldap
import utils
from retrying import retry


class GPTELdap(object):

    def __init__(self, logger):
        self.ldap_info = utils.get_secret_data('gpte-ldap-secrets')
        self.ldap_hosts = self.ldap_info['ldap_hosts']
        self.ldap_binddn = self.ldap_info['binddn']
        self.ldap_bindpw = self.ldap_info['bindpw']
        self.debug = False
        self.logger = logger
        self.ldap_conn = None

    # Wait 2^x * 500 milliseconds between each retry, up to 5 seconds, then 5 seconds afterwards and 3 attempts
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=500, wait_exponential_max=5000)
    def ldap_connect(self):
        try:
            self.ldap_conn = ldap.initialize("ldaps://{ldap_hosts}".format(ldap_hosts=self.ldap_hosts))
            ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, ldap.OPT_X_TLS_NEVER)
            self.ldap_conn.protocol_version = ldap.VERSION3
            self.ldap_conn.simple_bind_s(self.ldap_binddn, self.ldap_bindpw)
        except ldap.INVALID_CREDENTIALS:
            self.logger.error("Your username or password is incorrect.")
        except ldap.LDAPError as e:
            self.logger.error(f"Error connectint to LDAP {e}")

    def parse_ldap_result(self, result_data):
        user_data = {}
        for dn, entry in result_data:
            for k, v in entry.items():
                user_data.update({k: v[0].decode('utf-8')})

        return user_data

    def ldap_search_manager(self, manager_email):
        searchAttribute = self.ldap_info['searchattribute'].split(',')
        searchScope = ldap.SCOPE_SUBTREE
        bases_dn = [self.ldap_info['basedn'], self.ldap_info['basedndeleted']]
        searchFilter = f"(&(|(mail={manager_email})(rhatPreferredAlias={manager_email}))(objectClass=posixAccount))"
        user_data = {}
        try:
            for basedn in bases_dn:
                ldap_result_id = self.ldap_conn.search(basedn, searchScope, searchFilter, searchAttribute)
                while 1:
                    result_type, result_data = self.ldap_conn.result(ldap_result_id, 0)
                    if len(result_data) == 0:
                        break
                    else:
                        if result_type == ldap.RES_SEARCH_ENTRY:
                            user_data = self.parse_ldap_result(result_data)
                        return user_data
        except ldap.LDAPError as e:
            print(e)

        return user_data

    def ldap_search_user(self, email):
        if self.ldap_conn is None:
            self.ldap_connect()
        searchAttribute = self.ldap_info['searchattribute'].split(',')
        searchScope = ldap.SCOPE_SUBTREE
        bases_dn = [self.ldap_info['basedn'], self.ldap_info['basedndeleted']]
        searchFilter = f"(&(|(mail={email})(rhatPreferredAlias={email}))(objectClass=posixAccount))"
        user_data = {}
        try:
            for basedn in bases_dn:
                ldap_result_id = self.ldap_conn.search(basedn, searchScope, searchFilter, searchAttribute)
                user_data = {}
                while 1:
                    result_type, result_data = self.ldap_conn.result(ldap_result_id, 0)
                    if len(result_data) == 0:
                        break
                    else:
                        if result_type == ldap.RES_SEARCH_ENTRY:
                            for dn, entry in result_data:
                                user_data = self.parse_ldap_result(result_data)
                                if 'manager' in searchAttribute:
                                    manager_dn = entry['manager'][0].decode('utf-8')
                                    manager_uid = manager_dn.split(',')[0].replace('uid=', '')
                                    manager_email = manager_uid + '@redhat.com'
                                    user_data['manager'] = self.ldap_search_manager(manager_email)
                        return user_data
        except ldap.LDAPError as e:
            print(e)

        if self.debug:
            print("USER LDAP Results: ", user_data)

        return user_data

    def convert_dn_email(self, entry):
        manager_dn = entry['manager'][0].decode('utf-8')
        manager_uid = manager_dn.split(',')[0].replace('uid=', '')
        manager_email = manager_uid + '@redhat.com'
        return manager_email

    def ldap_user_headcount(self, email, managers, manager_email=None, count=1):
        # ldap_conn = ldap_connect()
        searchAttribute = ["manager"]
        searchScope = ldap.SCOPE_SUBTREE
        bases_dn = [self.ldap_info['basedn'], self.ldap_info['basedndeleted']]
        if manager_email:
            searchFilter = f"(&(|(mail={manager_email})(rhatPreferredAlias={manager_email}))(objectClass=posixAccount))"
        else:
            searchFilter = f"(&(|(mail={email})(rhatPreferredAlias={email}))(objectClass=posixAccount))"
        user_data = {}
        if self.ldap_conn is None:
            self.ldap_connect()
        try:
            for basedn in bases_dn:
                ldap_result_id = self.ldap_conn.search(basedn, searchScope, searchFilter, searchAttribute)
                user_data = {}
                while 1:
                    result_type, result_data = self.ldap_conn.result(ldap_result_id, 0)
                    if len(result_data) == 0:
                        self.logger.warning(f"User {email} not found in {basedn}. Trying another base")
                        break
                    else:
                        if result_type == ldap.RES_SEARCH_ENTRY:
                            for dn, entry in result_data:
                                user_data = self.parse_ldap_result(result_data)
                                if 'manager' in searchAttribute:
                                    manager_email = self.convert_dn_email(entry)
                                    # TODO: I have to create a new way to get the top level
                                    if manager_email == 'bod@redhat.com' or \
                                            manager_email == 'pcormier@redhat.com' or \
                                            manager_email == 'mhicks@redhat.com':
                                        return 'gpte@redhat.com'
                                    if manager_email in managers:
                                        return manager_email
                                    else:
                                        if manager_email == 'bod@redhat.com' or \
                                                manager_email == 'pcormier@redhat.com' or \
                                                manager_email == 'mhicks@redhat.com' or \
                                                isinstance(manager_email, dict):
                                            return 'gpte@redhat.com'
                                        if manager_email in managers:
                                            return manager_email
                                        manager_email = self.ldap_user_headcount(email, managers, manager_email, count)
                                        return manager_email
        except ldap.LDAPError as e:
            print(e)

        return user_data
