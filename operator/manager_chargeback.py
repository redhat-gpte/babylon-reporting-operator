import utils


class ManagerChargeback(object):

    def __init__(self, logger):
        self.debug = True
        self.logger = logger

    def list_manager(self):
        manager_list = {}
        query = "SELECT email, id from manager_chargeback"
        try:
            result = utils.execute_query(query, autocommit=True)
            for m in result['query_result']:
                manager_list.update({m['email']: m['id']})
        except Exception:
            self.logger.error("Error getting list of manager chargeback", stack_info=True)

        return manager_list
