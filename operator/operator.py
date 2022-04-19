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


def handle_anarchy_events(logger, anarchy_subject, resource_vars):
    possible_states = [
        'None',
        'new',
        'provision-pending',
        'provisioning',
        'provision',
        'provision-failed' ,
        'started',
        'start-pending',
        'starting',
        'start-failed',
        'stop-pending',
        'stopping',
        'stop-failed',
        'stopped',
        'destroying',
        'destroy-failed',
        'destroy-canceled',
    ]

    resource_current_state = resource_vars.get('current_state')
    resource_desired_state = resource_vars.get('desired_state')
    resource_claim_uuid = resource_vars.get('resource_claim_uuid')
    resource_claim_requester = resource_vars.get('resource_claim_requester')

    if resource_current_state not in possible_states or resource_current_state in ('new', None):
        logger.warning(f"Current state '{resource_current_state}' not found. Provision UUID: {resource_claim_uuid}")
        logger.info(f"Ignore action for {resource_claim_uuid} - {resource_vars}")
        return

    last_action = utils.last_lifecycle(resource_claim_uuid)

    log_info = {'last_action': last_action,
                'provision_uuid': resource_claim_uuid,
                'current_state': resource_current_state,
                'desired_state': resource_desired_state}

    logger.info(f"Handle event provision {resource_current_state} {log_info}")

    populate_provision(logger, anarchy_subject, resource_vars)

    last_action = utils.last_lifecycle(resource_claim_uuid)

    # Update provision_results if the last action was provision
    logger.info(f"handle_anarchy_events: {log_info}:")
    if last_action and last_action.startswith('provision') and 'failed' in resource_current_state:
        logger.info("Last action was provision, updating provision_result")
        utils.provision_lifecycle(resource_claim_uuid, resource_current_state, resource_claim_requester)
        utils.update_provision_result(resource_claim_uuid, 'failure')

    if last_action == 'provisioning' and 'completed' in resource_current_state:
        utils.provision_lifecycle(resource_claim_uuid, 'provision-completed', resource_claim_requester)
    else:
        utils.provision_lifecycle(resource_claim_uuid, resource_current_state, resource_claim_requester)


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

    invalid_states = ['new', 'provision-pending', 'provisioning']

    resource_vars = get_resource_vars(anarchy_subject)
    resource_current_state = resource_vars.get('current_state')
    resource_desired_state = resource_vars.get('desired_state')
    resource_claim_uuid = resource_vars.get('resource_claim_uuid')
    resource_claim_requester = resource_vars.get('resource_claim_requester')
    resource_claim_name = resource_vars.get('resource_claim_name')
    resource_claim_namespace = resource_vars.get('resource_claim_namespace')

    if not resource_current_state or resource_current_state in invalid_states:
        if resource_current_state == 'provisioning':
            utils.provision_lifecycle(resource_claim_uuid, resource_current_state, resource_claim_requester)

        logger.info(f"Provision: {resource_claim_uuid} - "
                    f"Current State: '{resource_current_state}'. "
                    f"We have to ignore it!")
        return

    if resource_current_state == resource_desired_state:
        logger.info(f"No update required for {resource_claim_uuid} - "
                    f"Current State: {resource_current_state} - "
                    f"Desired State: {resource_desired_state}")
        return

    # TODO: Check if tower jobs is completed
    if event['type'] == 'DELETED' and resource_current_state == 'destroying':

        positional_args = [datetime.now(timezone.utc), resource_claim_uuid]
        logger.info(f"Set retirement date for provision {resource_claim_uuid} - {datetime.now(timezone.utc)}")
        query = f"UPDATE provisions SET retired_at = %s \n" \
                f"WHERE uuid = %s and retired_at ISNULL RETURNING uuid;"

        utils.execute_query(query, positional_args=positional_args, autocommit=True)

        utils.provision_lifecycle(resource_claim_uuid, 'destroy-completed', resource_claim_requester)

        return
    else:
        handle_anarchy_events(logger, anarchy_subject, resource_vars)

    utils.save_anarchy_subject(resource_claim_uuid, resource_claim_name, resource_claim_namespace, anarchy_subject)


def populate_provision(logger, anarchy_subject, resource_vars):
    invalid_states = ['provision-pending']

    resource_current_state = resource_vars.get('current_state')
    resource_claim_uuid = resource_vars.get('resource_claim_uuid')
    logger.info(f"LOGGER populate_provision: {resource_claim_uuid} - {resource_vars}")

    if resource_current_state in invalid_states:
        return

    if not resource_claim_uuid:
        logger.error("Provision UUID is None")
        return

    provision = prepare(anarchy_subject, logger, resource_vars)

    logger.info(f"Populate Provision: {provision}")

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
    anarchy_subject_status = anarchy_subject.get('status', {})

    resource_label_governor = anarchy_subject_spec.get('governor', '')
    anarchy_subject_name = anarchy_subject_metadata.get('name')
    current_state = anarchy_subject_spec_vars.get('current_state')

    resource_uuid = anarchy_subject_job_vars.get('uuid',
                                                 anarchy_subject_annotations.get(
                                                     f"{poolboy_domain}/resource-handle-uid")
                                                 )
    resource_claim_namespace = anarchy_subject_annotations.get(f"{poolboy_domain}/resource-claim-namespace")
    resource_claim_name = anarchy_subject_annotations.get(f"{poolboy_domain}/resource-claim-name")

    # Get user name from poolboy annotation and fallback to namespace name
    resource_claim_requester = anarchy_subject_annotations.get(f"{babylon_domain}/requester",
                                               anarchy_subject_annotations.get(
                                                   f"{poolboy_domain}/resource-requester-user")
                                               )

    if resource_claim_requester is None and 'empty-config' in resource_label_governor:
        resource_claim_requester = 'poolboy'

    # If we don't have resource_claim_name it means that the provision has been deployed using poolbooy
    if not resource_claim_namespace:
        resource_claim_requester = 'poolboy'

    resource_claim_requester_email = anarchy_subject_annotations.get(f"{babylon_domain}/requester-email")

    desired_state = anarchy_subject_spec_vars.get('desired_state')

    provision_data = anarchy_subject_spec_vars.get('provision_data', {})
    tower_jobs = anarchy_subject_status.get('towerJobs', {})
    provision_job = tower_jobs.get('provision', {})
    job_vars = anarchy_subject_spec_vars.get('job_vars', {})

    sandbox_account = anarchy_subject_job_vars.get('sandbox_account', provision_data.get('ibm_sandbox_account'))
    sandbox_name = anarchy_subject_job_vars.get('sandbox_name', provision_data.get('ibm_sandbox_name'))

    babylon_guid = provision_job.get('guid', anarchy_subject_job_vars.get('guid')),
    cloud_region = provision_job.get('region', anarchy_subject_job_vars.get('region'))

    kw = {
        'current_state': current_state,
        'desired_state': desired_state,
        'resource_claim_uuid': resource_uuid,
        'username': resource_claim_requester,
        'resource_claim_requester': resource_claim_requester,
        'resource_claim_requester_email': resource_claim_requester_email,
        'babylon_guid': babylon_guid,
        'tower_jobs': tower_jobs,
        'provision_job': provision_job,
        'provision_data': provision_data,
        'sandbox_name': sandbox_name,
        'sandbox_account': sandbox_account,
        'cloud_region': cloud_region,
        'job_vars': job_vars,
        'anarchy_subject_name': anarchy_subject_name,
        'resource_claim_namespace': resource_claim_namespace,
        'resource_claim_name': resource_claim_name,
        'resource_label_governor': resource_label_governor
    }

    return kw


def prepare(anarchy_subject, logger, resource_vars):

    resource_current_state = resource_vars.get('current_state')
    resource_desired_state = resource_vars.get('desired_state')
    resource_claim_uuid = resource_vars.get('resource_claim_uuid')
    resource_claim_requester = resource_vars.get('resource_claim_requester')
    resource_claim_name = resource_vars.get('resource_claim_name')
    resource_claim_namespace = resource_vars.get('resource_claim_namespace')
    resource_label_governor = resource_vars.get('resource_label_governor')

    catalog_display_name = parse_catalog_item(resource_label_governor)
    catalog_item_display_name = parse_catalog_item(resource_label_governor)

    provision_data = resource_vars.get('provision_data')
    provision_job = resource_vars.get('provision_job')
    provision_job_id = provision_job.get('deployerJob')
    provision_job_url = provision_job.get('towerJobURL')

    provision_job_start_timestamp = utils.timestamp_to_utc(provision_job.get('startTimestamp'))
    provision_job_complete_timestamp = utils.timestamp_to_utc(provision_job.get('completeTimestamp'))

    class_list = resource_label_governor.split('.')
    class_name = f"{class_list[2]}_{class_list[1].replace('-', '_')}".upper()

    chargeback_method = 'regional'
    sales_force_id = None
    purpose = None
    notifier = False
    resource_guid = None
    provision_time = 0
    deploy_interval = None
    provision_job_vars = {}
    platform_url = None
    using_cloud_forms = False


    logger.info(f"Resource claim UUID: {resource_claim_uuid} - resource_label_governor: {resource_label_governor}")
    logger.info(f"Resource UUID: {resource_claim_uuid} - "
                f"Resource Current State: {resource_current_state} - "
                f"Resource Desired State: {resource_desired_state}")

    # If we have resource_claim_namespace we have user associated
    if resource_claim_name and resource_claim_namespace and \
            resource_current_state not in ('destroying', 'destroy-failed', 'starting'):
        try:
            resource_claim = custom_objects_api.get_namespaced_custom_object(
                poolboy_domain, poolboy_api_version,
                resource_claim_namespace, 'resourceclaims', resource_claim_name
            )

            # TODO: Remove debug messages
            logger.debug("RESOURCE CLAIM LOG:")
            logger.info(json.dumps(resource_claim, default=str))

            resource_claim_metadata = resource_claim['metadata']
            resource_claim_annotations = resource_claim_metadata['annotations']
            resource_claim_labels = resource_claim_metadata['labels']

            utils.save_resource_claim_data(resource_claim_uuid, resource_claim_name,
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
                                          parse_catalog_item(resource_label_governor))
            )

            catalog_item_display_name = resource_claim_annotations.get(
                f"{babylon_domain}/catalogItemDisplayName",
                resource_claim_labels.get(f"{babylon_domain}/catalogItemName",
                                          parse_catalog_item(resource_label_governor))
            )

            # Purpose and SalesForce Opportunity
            sales_force_id = resource_claim_annotations.get(f"{pfe_domain}/salesforce-id")

            # Purpose
            purpose = resource_claim_annotations.get(f"{pfe_domain}/purpose")

        except ApiException as e:
            if e.status == '404':
                logger.info(f"Resource Claim not found {resource_claim_name} "
                               f"from namespace {resource_claim_namespace} for provision "
                               f"UUID {resource_claim_uuid} - current_state: {resource_current_state}  - {e.status}")
                pass
            logger.warning(f"Unable to get namespace custom object resource claim {resource_claim_name} "
                           f"from namespace {resource_claim_namespace} for provision "
                           f"UUID {resource_claim_uuid} - current_state: {resource_current_state} - {e}")
            pass

    logger.info(f"Provision UUID: {resource_claim_uuid} "
                f"catalog_display_name: {catalog_display_name} "
                f"catalog_item_display_name: {catalog_item_display_name}")

    if provision_job_start_timestamp:
        # if provision has no completed, using current datetime as completed time
        if not provision_job_complete_timestamp:
            provision_time = (datetime.now(timezone.utc) - provision_job_start_timestamp).total_seconds() / 60.0
            deploy_interval = datetime.now(timezone.utc) - provision_job_start_timestamp
        else:
            provision_time = (provision_job_complete_timestamp - provision_job_start_timestamp).total_seconds() / 60.0
            deploy_interval = provision_job_complete_timestamp - provision_job_start_timestamp

        logger.debug(f"Provision Time in Minutes: {provision_time} - Provision Time Interval: {deploy_interval}")

    if provision_job_id:
        resp = requests.get(
            f"https://{ansible_tower_hostname}/api/v2/jobs/{provision_job_id}",
            auth=(ansible_tower_user, ansible_tower_password),
            # We really need to fix the tower certs!
            verify=False,
        )
        provision_tower_job = resp.json()
        provision_job_vars = json.loads(provision_tower_job.get('extra_vars', '{}'))
        utils.save_tower_extra_vars(resource_claim_uuid, resource_claim_name, resource_claim_namespace,
                                  provision_job_vars)

        # If resource_claim_requester is null try to get it from provision_job_vars
        if not resource_claim_requester:
            resource_claim_requester = provision_job_vars.get('requester_username')


    babylon_guid = provision_job_vars.get('guid', resource_vars.get('babylon_guid'))
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

    azure_tenant = provision_data.get('azure_subscription')
    azure_subscription = provision_data.get('azure_subscription')

    if cloud == 'azure':
        sandbox_name = provision_data.get('sandbox_name')
    else:
        sandbox_account = resource_vars.get('sandbox_account')
        sandbox_name = resource_vars.get('sandbox_name')

    agnosticd_open_environment = provision_job_vars.get('agnosticd_open_environment', False)
    if agnosticd_open_environment:
        chargeback_method = 'open'

    # This is a fallback if agnosticd_open_environment is False
    if not agnosticd_open_environment and 'Open Environment' in catalog_item_display_name:
        chargeback_method = 'open'

    if purpose is None:
        purpose = provision_job_vars.get('purpose', 'Development - Catalog item creation / maintenance')

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
        'babylon_guid': babylon_guid,
        'cloud_region': provision_job_vars.get('region', resource_vars.get('cloud_region')),
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
        'anarchy_subject_name': resource_vars.get('anarchy_subject_name'),
        'platform_url': platform_url,
        'azure_tenant': azure_tenant,
        'azure_subscription': azure_subscription,
        'using_cloud_forms': using_cloud_forms
    }

    utils.save_provision_vars(resource_claim_uuid, resource_claim_name, resource_claim_namespace, provision)
    logger.info(f"Provision Details: {provision}")

    return provision
