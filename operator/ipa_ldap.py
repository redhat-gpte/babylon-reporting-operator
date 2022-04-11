import ldap
import utils
import json
from retrying import retry


class GPTEIpaLdap(object):

    def __init__(self, logger):
        self.ldap_info = utils.get_secret_data('gpte-ipa-secrets')
        self.ldap_hosts = self.ldap_info['ldap_hosts']
        self.ldap_binddn = self.ldap_info['binddn']
        self.ldap_bindpw = self.ldap_info['bindpw']
        self.ldap_basedn = self.ldap_info['basedn']
        self.debug = False
        self.log = logger
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
            self.log.error("Your username or password is incorrect.")
        except ldap.LDAPError as e:
            self.log.error(f"Error connectint to LDAP {e}")

    def search_ipa_user(self, user_name, attribute='uid'):
        """
        This method search for a *user_name* into GPTE IPA LDAP
        :param user_name: a valid username in GPTE IPA LDAP
        :return: A dictionary with user information from LDAP
        """
        if self.ldap_conn is None:
            self.ldap_connect()

        # self.log.info(f"Searching User {user_name} in {self.ldap_hosts} and base dn {self.ldap_basedn}")
        search_scope = ldap.SCOPE_SUBTREE
        search_filter = f"(&({attribute}={user_name})(objectClass=posixAccount))"
        search_attr = self.ldap_info['searchattribute'].split(',')

        ldap_result_id = self.ldap_conn.search(self.ldap_basedn, search_scope, search_filter, search_attr)

        result_type, result_data = self.ldap_conn.result(ldap_result_id, 0)

        user_data = utils.parse_ldap_result(result_data)
        if 'title' not in user_data:
            user_data['title'] = 'other'
        user_data['region'] = self.search_user_region(user_name)
        user_data['partner'] = self.search_user_partner(user_name)
        user_data['cost_center'] = None
        user_data['manager_chargeback_id'] = None
        user_data['manager'] = {
            'manager': None,
            'manager_email': None,
            'manager_id': None}

        return user_data

    def search_user_region(self, user_name):
        """
        By default RHDPS uses a group name to know which user's region, we have to find the group name `rhpds-geo-{geo}`

        :param user_name: IPA LDAP username
        :return: region_name str
        """
        if self.ldap_conn is None:
            self.ldap_connect()

        search_scope = ldap.SCOPE_SUBTREE
        search_filter = f'(&(cn=*geo*)(member=uid={user_name}*))'
        search_attr = ['cn']
        basedn = self.ldap_basedn.replace('cn=users', 'cn=groups')

        ldap_result_id = self.ldap_conn.search(basedn, search_scope, search_filter, search_attr)

        result_type, result_data = self.ldap_conn.result(ldap_result_id, 0)

        user_data = utils.parse_ldap_result(result_data)
        if self.debug:
            self.log.info(f"search_user_region: \n{json.dumps(user_data, indent=2)}")
        if 'cn' in user_data:
            region_name = user_data['cn'].split('-')[2].upper()
        else:
            region_name = 'NA'

        return region_name

    def search_user_partner(self, user_name):
        """
        By default RHDPS uses a group name to know which user's region, we have to find the group name `rhpds-geo-{geo}`

        :param user_name: IPA LDAP username
        :return: region_name str
        """
        if self.ldap_conn is None:
            self.ldap_connect()

        search_scope = ldap.SCOPE_SUBTREE
        search_filter = f'(&(cn=*partner*)(member=uid={user_name}*))'
        search_attr = ['cn']
        basedn = self.ldap_basedn.replace('cn=users', 'cn=groups')

        ldap_result_id = self.ldap_conn.search(basedn, search_scope, search_filter, search_attr)

        result_type, result_data = self.ldap_conn.result(ldap_result_id, 0)

        user_data = utils.parse_ldap_result(result_data)
        if 'cn' in user_data:
            partner_name = user_data['cn'].split('-')[2].upper()
        else:
            partner_name = 'partner'

        return partner_name

