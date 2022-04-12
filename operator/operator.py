#!/usr/bin/env python3
import os

# Set default timezone to UTC
os.environ['TZ'] = 'UTC'
os.environ['PGTZ'] = 'UTC'

import json
import kopf
import kubernetes
import requests
import urllib3
from kubernetes.client.rest import ApiException
from base64 import b64decode
from datetime import datetime, timezone
import utils
from ipa_ldap import GPTEIpaLdap
from corp_ldap import GPTELdap
from users import Users
from catalog_items import CatalogItems
from provisions import Provisions

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

anarchy_domain = os.environ.get('ANARCHY_DOMAIN', 'anarchy.gpte.redhat.com')
anarchy_api_version = os.environ.get('ANARCHY_API_VERSION', 'v1')
babylon_domain = os.environ.get('BABYLON_DOMAIN', 'babylon.gpte.redhat.com')
babylon_api_version = os.environ.get('BABYLON_API_VERSION', 'v1')
poolboy_domain = os.environ.get('POOLBOY_DOMAIN', 'poolboy.gpte.redhat.com')
poolboy_api_version = os.environ.get('POOLBOY_API_VERSION', 'v1')
pfe_domain = os.environ.get('PFE_DOMAIN', 'pfe.redhat.com')


if os.path.exists('/run/secrets/kubernetes.io/serviceaccount'):
    kubernetes.config.load_incluster_config()
else:
    kubernetes.config.load_kube_config()

core_v1_api = kubernetes.client.CoreV1Api()
custom_objects_api = kubernetes.client.CustomObjectsApi()
namespaces = {}

# TODO: Move events out of operator.py


def handle_no_event(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)
    logger.info(f"Ignore action for the state '{current_state}'")


def handle_event_provision_pending(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    logger.info(f"Handle event provision pending for {resource_uuid}.")

    populate_provision(logger, anarchy_subject)

    utils.provision_lifecycle(resource_uuid, current_state, username)


def handle_event_provisioning(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    logger.info(f"Handle event provisioning for {resource_uuid}.")

    utils.provision_lifecycle(resource_uuid, current_state, username)


def handle_event_provision_failed(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    logger.info(f"Handle event provision failed for {resource_uuid}.")

    populate_provision(logger, anarchy_subject)

    last_action = utils.last_lifecycle(resource_uuid)

    # Update provision_results if the last action was provision
    if last_action and last_action.startswith('provision'):
        logger.info("Last action was provision, updating provision_result")
        utils.update_provision_result(resource_uuid, 'failure')

    utils.provision_lifecycle(resource_uuid, current_state, username)


def handle_event_provision_complete(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    logger.info(f"Handle event provision complete for {resource_uuid}.")

    populate_provision(logger, anarchy_subject)

    utils.provision_lifecycle(resource_uuid, current_state, username)


def handle_event_started(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    logger.info(f"Handle event started for {resource_uuid}.")

    populate_provision(logger, anarchy_subject)

    last_state = utils.last_lifecycle(resource_uuid)
    if last_state == 'provisioning':
        utils.provision_lifecycle(resource_uuid, 'provision-completed', username)

    utils.provision_lifecycle(resource_uuid, current_state, username)


def handle_event_start_pending(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    logger.info(f"Handle event start pending for {resource_uuid}.")

    populate_provision(logger, anarchy_subject)

    utils.provision_lifecycle(resource_uuid, current_state, username)


def handle_event_starting(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    logger.info(f"Handle event starting for {resource_uuid}.")

    populate_provision(logger, anarchy_subject)

    utils.provision_lifecycle(resource_uuid, current_state, username)


def handle_event_start_failed(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    logger.info(f"Handle event start failed for {resource_uuid}.")

    populate_provision(logger, anarchy_subject)

    last_state = utils.last_lifecycle(resource_uuid)
    if last_state == 'provisioning':
        utils.provision_lifecycle(resource_uuid, 'provision-failed', username)

    last_action = utils.last_lifecycle(resource_uuid)

    # if last action was provision we have to update provision_results
    if last_action.startswith('provision'):
        logger.info("Last action was provision, needs to update provision_results")
        utils.update_provision_result(resource_uuid, 'failure')

    utils.provision_lifecycle(resource_uuid, current_state, username)


def handle_event_stop_pending(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    logger.info(f"Handle event stop pending for {resource_uuid}.")

    populate_provision(logger, anarchy_subject)

    utils.provision_lifecycle(resource_uuid, current_state, username)


def handle_event_stopping(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    logger.info(f"Handle event stopping for {resource_uuid}.")

    populate_provision(logger, anarchy_subject)

    utils.provision_lifecycle(resource_uuid, current_state, username)


def handle_event_stop_failed(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    logger.info(f"Handle event stop failed for {resource_uuid}.")

    populate_provision(logger, anarchy_subject)

    utils.provision_lifecycle(resource_uuid, current_state, username)


def handle_event_stopped(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    logger.info(f"Handle event stopped for {resource_uuid}.")

    populate_provision(logger, anarchy_subject)

    utils.provision_lifecycle(resource_uuid, current_state, username)


def handle_event_destroying(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    logger.info(f"Handle event destroying for {resource_uuid}.")

    populate_provision(logger, anarchy_subject)

    utils.provision_lifecycle(resource_uuid, current_state, username)


def handle_event_destroy_failed(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    logger.info(f"Handle event destroy failed for {resource_uuid}.")

    populate_provision(logger, anarchy_subject)

    utils.provision_lifecycle(resource_uuid, current_state, username)


def handle_event_destroy_canceled(logger, anarchy_subject):
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    logger.info(f"Handle event destroy failed for {resource_uuid}.")

    populate_provision(logger, anarchy_subject)

    utils.provision_lifecycle(resource_uuid, current_state, username)


resource_states = {
    'None': handle_no_event,
    'new': handle_no_event,
    'provision-pending': handle_event_provision_pending,
    'provisioning': handle_event_provisioning,
    'provision-failed': handle_event_provision_failed,
    'started': handle_event_started,
    'start-pending': handle_event_start_pending,
    'starting': handle_event_starting,
    'start-failed': handle_event_start_failed,
    'stop-pending': handle_event_stop_pending,
    'stopping': handle_event_stopping,
    'stop-failed': handle_event_stop_failed,
    'stopped': handle_event_stopped,
    'destroying': handle_event_destroying,
    'destroy-failed': handle_event_destroy_failed,
    'destroy-canceled': handle_event_destroy_canceled
}


@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    global ansible_tower_hostname, ansible_tower_password, ansible_tower_user

    # Disable scanning for CustomResourceDefinitions
    settings.scanning.disabled = True

    # Get the tower secret. This may change in the future if there are
    # multiple ansible tower deployments
    ansible_tower_secret = core_v1_api.read_namespaced_secret('babylon-tower', 'anarchy-operator')
    ansible_tower_hostname = b64decode(ansible_tower_secret.data['hostname']).decode('utf8')
    ansible_tower_password = b64decode(ansible_tower_secret.data['password']).decode('utf8')
    ansible_tower_user = b64decode(ansible_tower_secret.data['user']).decode('utf8')


@kopf.on.event(
    'namespaces',
)
def namespace_event(event, logger, **_):
    namespace = event.get('object')

    # Only respond to events that include Namespace data.
    if not namespace \
    or namespace.get('kind') != 'Namespace':
        logger.warning(event)
        return

    name = namespace['metadata']['name']
    namespaces[name] = namespace

@kopf.on.event(
    anarchy_domain, anarchy_api_version, 'anarchysubjects',
)
def anarchysubject_event(event, logger, **_):
    anarchy_subject = event.get('object')

    # Only respond to events that include AnarchySubject data.
    if not anarchy_subject \
    or anarchy_subject.get('kind') != 'AnarchySubject':
        logger.warning(event)
        return

    # TODO: Remove debug message after deploy in production
    logger.info(f"DEBUG anarchy_subject: {anarchy_subject}")

    anarchy_subject_spec = anarchy_subject['spec']
    anarchy_subject_spec_vars = anarchy_subject_spec['vars']

    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)

    # TODO: Check if tower jobs is completed
    if event['type'] == 'DELETED' and current_state == 'destroying':

        positional_args = [datetime.now(timezone.utc), resource_uuid]
        logger.info(f"Set retirement date for provision {resource_uuid} - {datetime.now(timezone.utc)}")
        query = f"UPDATE provisions SET retired_at = %s \n" \
                f"WHERE uuid = %s and retired_at ISNULL RETURNING uuid;"

        utils.execute_query(query, positional_args=positional_args, autocommit=True)

        utils.provision_lifecycle(resource_uuid, 'destroy-completed', username)

        return

    if current_state in resource_states:
        resource_states[current_state](logger, anarchy_subject)
    else:
        logger.warning(f"Current state '{current_state}' not found. Provision UUID: {resource_uuid}")
        return

    if not current_state or current_state in ('new', 'provision-pending'):
        logger.warning(f"Provision: {resource_uuid} - "
                       f"Current State: '{anarchy_subject_spec_vars.get('current_state')}'. "
                       f"We have to ignore it!")
        return


def populate_provision(logger, anarchy_subject):
    invalid_states = ['provision-pending']
    current_state, desired_state, resource_uuid, username, babylon_guid = get_resource_vars(anarchy_subject)
    if current_state in invalid_states:
        return

    provision = prepare(anarchy_subject, logger)
    if provision:

        user_name = provision.get('username')
        if user_name is None:
            logger.warning(f"Unable to get username for provision {provision.get('uuid')} - "
                           f"Current State: {provision.get('current_state')} - "
                           f"anarchy_subject_name: {provision.get('anarchy_subject_name')} -"
                           f"anarchy_governor: {provision.get('anarchy_governor')}")
            provision['user'] = {}
        else:
            provision['user'] = search_ipa_user(user_name, logger, provision.get('using_cloud_forms', False))

        provision['user_db'] = populate_user(provision, logger)
        provision['catalog_id'] = populate_catalog(provision, logger)

        prov = Provisions(logger, provision)
        prov.populate_provisions()


def populate_catalog(provision, logger):
    catalog = CatalogItems(logger, provision)
    results = catalog.populate_catalog_items()
    return results


def populate_user(provision, logger):
    users = Users(logger, provision)
    user_data = provision.get('user', {})
    user_email = user_data.get('mail')
    if not user_email:
        results = {'user_id': None,
                   'manager_chargeback_id': None,
                   'manager_id': None,
                   'cost_center': None
         }
    else:
        results = users.populate_users()

    return results


def search_ipa_user(user_name, logger, notifier=False):

    if '@redhat' in user_name and not notifier:
        logger.info(f"Searching CORP LDAP username '{user_name}'")
        corp_ldap = GPTELdap(logger)
        results = corp_ldap.ldap_search_user(user_name)
    else:
        logger.info(f"Searching IPA username '{user_name}'")
        int_ldap = GPTEIpaLdap(logger)
        if notifier and '@' in user_name:
            logger.info(f"Searching IPA username using mail '{user_name}'")
            results = int_ldap.search_ipa_user(user_name, 'mail')
        else:
            logger.info(f"Searching IPA username using uid '{user_name}'")
            results = int_ldap.search_ipa_user(user_name)

    logger.debug(f"DEBUG USER: {results}")
    return results


def parse_catalog_item(catalog_display_name):
    ci_name = catalog_display_name

    if '.' in catalog_display_name:
        name_list = catalog_display_name.split('.')
        ci_name = name_list[1]

    return ci_name.strip()


def get_resource_vars(anarchy_subject):
    anarchy_subject_spec = anarchy_subject['spec']
    anarchy_subject_spec_vars = anarchy_subject_spec['vars']
    anarchy_subject_job_vars = anarchy_subject_spec_vars.get('job_vars', {})
    anarchy_subject_metadata = anarchy_subject['metadata']
    anarchy_subject_annotations = anarchy_subject_metadata['annotations']
    resource_label_governor = anarchy_subject_spec.get('governor', '')

    current_state = anarchy_subject_spec_vars.get('current_state')

    resource_uuid = anarchy_subject_job_vars.get('uuid',
                                                 anarchy_subject_annotations.get(
                                                     f"{poolboy_domain}/resource-handle-uid")
                                                 )

    resource_claim_namespace = anarchy_subject_annotations.get(f"{poolboy_domain}/resource-claim-namespace")

    # Get user name from poolboy annotation and fallback to namespace name
    username = anarchy_subject_annotations.get(f"{babylon_domain}/requester")

    if username is None:
        username = anarchy_subject_annotations.get(
            f"{poolboy_domain}/resource-requester-user")

    if resource_claim_namespace and not username:
        replace = '.'
        temp_username = resource_claim_namespace.replace('user-', '')
        username = replace.join(temp_username.rsplit('-', 1))

    if username is None and 'empty-config' in resource_label_governor:
        username = 'poolboy'

    if not resource_claim_namespace:
        username = 'poolboy'

    desired_state = anarchy_subject_spec_vars.get('desired_state')

    babylon_guid = anarchy_subject_job_vars.get('guid')

    return current_state, desired_state, resource_uuid, username, babylon_guid


def prepare(anarchy_subject, logger):
    anarchy_subject_spec = anarchy_subject['spec']
    anarchy_subject_spec_vars = anarchy_subject_spec['vars']
    anarchy_subject_metadata = anarchy_subject['metadata']
    anarchy_subject_annotations = anarchy_subject_metadata['annotations']
    anarchy_subject_labels = anarchy_subject_metadata['labels']
    provision_data = anarchy_subject_spec_vars.get('provision_data', {})
    anarchy_subject_job_vars = anarchy_subject_spec_vars.get('job_vars', {})
    anarchy_subject_status = anarchy_subject.get('status', {})
    tower_jobs = anarchy_subject_status.get('towerJobs', {})
    provision_job = tower_jobs.get('provision', {})
    provision_job_id = provision_job.get('deployerJob')
    provision_job_url = provision_job.get('towerJobURL')

    # This is the resource claim namespace
    as_resource_claim_name = anarchy_subject_annotations.get(f"{poolboy_domain}/resource-claim-name")
    resource_claim_namespace = anarchy_subject_annotations.get(f"{poolboy_domain}/resource-claim-namespace")

    # This is the resource UUID
    resource_claim_uuid = anarchy_subject_job_vars.get('uuid',
                                                       anarchy_subject_annotations.get(
                                                           f"{poolboy_domain}/resource-handle-uid")
                                                       )
    logger.info(f"Resource claim UUID: {resource_claim_uuid}")

    resource_label_governor = anarchy_subject_spec.get('governor')
    logger.info(f"resource_label_governor: {resource_label_governor}")

    resource_current_state = anarchy_subject_spec_vars.get('current_state')
    resource_desired_state = anarchy_subject_spec_vars.get('desired_state')
    logger.info(f"Resource UUID: {resource_claim_uuid} - "
                f"Resource Current State: {resource_current_state} - "
                f"Resource Desired State: {resource_desired_state}")

    catalog_display_name = parse_catalog_item(resource_label_governor)
    catalog_item_display_name = parse_catalog_item(resource_label_governor)
    resource_guid = None

    # Get user name from poolboy annotation and fallback to namespace name
    resource_claim_requester = anarchy_subject_annotations.get(f"{babylon_domain}/requester")

    if resource_claim_requester is None:
        resource_claim_requester = anarchy_subject_annotations.get(
            f"{poolboy_domain}/resource-requester-user")

    if resource_claim_namespace and not resource_claim_requester:
        replace = '.'
        temp_username = resource_claim_namespace.replace('user-', '')
        resource_claim_requester = replace.join(temp_username.rsplit('-', 1))

    logger.info(f"Provision UUID: {resource_claim_uuid} - "
                f"Resource Claim Namespace: {resource_claim_namespace} - "
                f"Resource Claim Name: {as_resource_claim_name} - "
                f"Resource Current State: {resource_current_state}")

    sales_force_id = None
    purpose = None

    # If we don't have resource_claim_name it means that the provision has been deployed using poolbooy
    if not resource_claim_namespace:
        resource_claim_requester = 'poolboy'

    notifier = False
    # If we have resource_claim_namespace we have user associated
    if as_resource_claim_name and resource_claim_namespace and \
            resource_current_state not in ('destroying', 'destroy-failed', 'starting'):
        try:
            resource_claim = custom_objects_api.get_namespaced_custom_object(
                poolboy_domain, poolboy_api_version,
                resource_claim_namespace, 'resourceclaims', as_resource_claim_name
            )

            logger.debug("RESOURCE CLAIM LOG:")
            logger.debug(json.dumps(resource_claim, default=str))
            resource_claim_metadata = resource_claim['metadata']
            resource_claim_annotations = resource_claim_metadata['annotations']
            resource_claim_labels = resource_claim_metadata['labels']
            if f'{babylon_domain}/requester' in resource_claim_annotations:
                resource_claim_requester = resource_claim_annotations.get(f'{babylon_domain}/requester')

            utils.save_resource_claim_data(resource_claim_uuid, as_resource_claim_name,
                                           resource_claim_namespace, resource_claim)

            # Used by CloudForms
            notifier = resource_claim_annotations.get(f'{babylon_domain}/externalPlatformUrl', False)
            if notifier:
                resource_name = resource_claim_metadata.get('name')
                if resource_name:
                    resource_guid = resource_name[-4:]

            # if babylon/catalogDisplayName get it from labels/{babylon_domain}/catalogItemName
            # else, try to get it from resource_claim lables or finally using resource_label_governor
            catalog_display_name = resource_claim_annotations.get(
                f"{babylon_domain}/catalogDisplayName",
                resource_claim_labels.get(f"{babylon_domain}/catalogItemName",
                                          catalog_item_display_name(resource_label_governor))
            )

            catalog_item_display_name = resource_claim_annotations.get(
                f"{babylon_domain}/catalogItemDisplayName",
                resource_claim_labels.get(f"{babylon_domain}/catalogItemName",
                                          catalog_item_display_name(resource_label_governor))
            )

            # Purpose and SalesForce Opportunity
            sales_force_id = resource_claim_annotations.get(f"{pfe_domain}/salesforce-id")

            # Purpose
            purpose = resource_claim_annotations.get(f"{pfe_domain}/purpose")

        except ApiException as e:
            if e.status == '404':
                logger.warning(f"Resource Claim not found {as_resource_claim_name} "
                               f"from namespace {resource_claim_namespace} for provision "
                               f"UUID {resource_claim_uuid} - current_state: {resource_current_state}  - {e.status}")
                pass
            logger.warning(f"Unable to get namespace custom object resource claim {as_resource_claim_name} "
                           f"from namespace {resource_claim_namespace} for provision "
                           f"UUID {resource_claim_uuid} - current_state: {resource_current_state} - {e}")
            pass

    logger.info(f"Provision UUID: {resource_claim_uuid} "
                f"catalog_display_name: {catalog_display_name} "
                f"catalog_item_display_name: {catalog_item_display_name}")

    provision_job_start_timestamp = utils.timestamp_to_utc(provision_job.get('startTimestamp'))
    provision_job_complete_timestamp = utils.timestamp_to_utc(provision_job.get('completeTimestamp'))

    provision_time = 0
    deploy_interval = None
    if provision_job_start_timestamp:
        # if provision has no completed, using current datetime as completed time
        if not provision_job_complete_timestamp:
            provision_time = (datetime.now(timezone.utc) - provision_job_start_timestamp).total_seconds() / 60.0
            deploy_interval = datetime.now(timezone.utc) - provision_job_start_timestamp
        else:
            provision_time = (provision_job_complete_timestamp - provision_job_start_timestamp).total_seconds() / 60.0
            deploy_interval = provision_job_complete_timestamp - provision_job_start_timestamp

        logger.debug(f"Provision Time in Minutes: {provision_time} - Provision Time Interval: {deploy_interval}")

    provision_job_vars = {}
    if provision_job_id:
        resp = requests.get(
            f"https://{ansible_tower_hostname}/api/v2/jobs/{provision_job_id}",
            auth=(ansible_tower_user, ansible_tower_password),
            # We really need to fix the tower certs!
            verify=False,
        )
        provision_tower_job = resp.json()
        provision_job_vars = json.loads(provision_tower_job.get('extra_vars', '{}'))
        utils.save_provision_vars(resource_claim_uuid, as_resource_claim_name, resource_claim_namespace,
                                  provision_job_vars)

    class_list = resource_label_governor.split('.')
    class_name = f"{class_list[2]}_{class_list[1].replace('-', '_')}".upper()

    sandbox_account = anarchy_subject_job_vars.get('sandbox_account', provision_data.get('ibm_sandbox_account'))
    sandbox_name = anarchy_subject_job_vars.get('sandbox_name', provision_data.get('ibm_sandbox_name'))

    workshop_users = provision_job_vars.get('user_count', provision_job_vars.get('num_users', 1))

    datasource = provision_job_vars.get('platform', 'BABYLON').upper()
    if datasource == 'LABS':
        datasource = 'OPENTLC'

    cloud = provision_job_vars.get('cloud_provider', 'test')
    if cloud == 'ec2':
        cloud = 'aws'
    elif cloud == 'osp':
        cloud = 'openstack'
    elif cloud == 'none':
        cloud = 'shared'

    chargeback_method = 'regional'
    if 'Open Environment' in catalog_item_display_name:
        chargeback_method = 'open'

    if purpose is None:
        purpose = provision_job_vars.get('purpose', 'Development - Catalog item creation / maintenance')

    platform_url = None
    using_cloud_forms = False
    if notifier:
        platform_url = notifier
        using_cloud_forms = True

    if '.' in catalog_display_name:
        catalog_display_name = parse_catalog_item(catalog_display_name)

    if '.' in catalog_item_display_name:
        catalog_item_display_name = parse_catalog_item(catalog_item_display_name)

    # Define a dictionary with all information from provisions
    provision = {
        'provisioned_at': provision_job_start_timestamp,
        'job_start_timestamp': provision_job_start_timestamp,
        'job_complete_timestamp': provision_job_complete_timestamp,
        'provision_time': provision_time,
        'deploy_interval': deploy_interval,
        'uuid': resource_claim_uuid,
        'username': resource_claim_requester,
        'catalog_id': resource_label_governor,
        'catalog_name': catalog_display_name,
        'catalog_item': catalog_item_display_name,
        'current_state': resource_current_state,
        'desired_state': resource_desired_state,
        'guid': resource_guid,
        'babylon_guid': provision_job_vars.get('guid', anarchy_subject_job_vars.get('guid')),
        'cloud_region': provision_job_vars.get('region', anarchy_subject_job_vars.get('region')),
        'cloud': cloud,
        'env_type': provision_job_vars.get('env_type', 'tests'),
        'datasource': datasource,
        'environment': class_list[2],
        'account': class_list[0],
        'class_name': class_name,
        'sandbox': sandbox_account,
        'sandbox_name': sandbox_name,
        'provision_vars': provision_job_vars,
        'manager_chargeback': 'default',
        'check_headcount': False,
        'opportunity': sales_force_id,
        'chargeback_method': chargeback_method,
        'workshop_users': workshop_users,
        'tower_job_id': provision_job_id,
        'tower_job_url': provision_job_url,
        'purpose': purpose,
        'anarchy_governor': resource_label_governor,
        'anarchy_subject_name': anarchy_subject_metadata.get('name'),
        'platform_url': platform_url,
        'using_cloud_forms': using_cloud_forms
    }

    logger.info(f"Provision Details: {provision}")

    return provision
