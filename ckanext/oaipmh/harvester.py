import logging

import oaipmh.client

import importformats

from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckan.model import Session
from ckanext.harvest.model import HarvestJob, HarvestObject
from ckanext.harvest.harvesters.base import HarvesterBase

log = logging.getLogger(__name__)


class OAIPMHHarvester(HarvesterBase):
    '''
    OAI-PMH Harvester
    '''

    def info(self):
        '''
        Harvesting implementations must provide this method, which will return a
        dictionary containing different descriptors of the harvester. The
        returned dictionary should contain:

        * name: machine-readable name. This will be the value stored in the
          database, and the one used by ckanext-harvest to call the appropiate
          harvester.
        * title: human-readable name. This will appear in the form's select box
          in the WUI.
        * description: a small description of what the harvester does. This will
          appear on the form as a guidance to the user.

        A complete example may be::

            {
                'name': 'csw',
                'title': 'CSW Server',
                'description': 'A server that implements OGC's Catalog Service
                                for the Web (CSW) standard'
            }

        :returns: A dictionary with the harvester descriptors
        '''

        log.debug("Entering info()")
        log.debug("Exiting info()")
        return {
            'name': 'oai-pmh',
            'title': 'OAI-PMH',
            'description': 'Harvests OAI-PMH providers'
        }

    # def validate_config(self, config):
    #     '''
    #
    #     [optional]
    #
    #     Harvesters can provide this method to validate the configuration entered in the
    #     form. It should return a single string, which will be stored in the database.
    #     Exceptions raised will be shown in the form's error messages.
    #
    #     :param harvest_object_id: Config string coming from the form
    #     :returns: A string with the validated configuration options
    #     '''

    # def get_original_url(self, harvest_object_id):
    #     '''
    #
    #     [optional]
    #
    #     This optional but very recommended method allows harvesters to return
    #     the URL to the original remote document, given a Harvest Object id.
    #     Note that getting the harvest object you have access to its guid as
    #     well as the object source, which has the URL.
    #     This URL will be used on error reports to help publishers link to the
    #     original document that has the errors. If this method is not provided
    #     or no URL is returned, only a link to the local copy of the remote
    #     document will be shown.
    #
    #     Examples:
    #         * For a CKAN record: http://{ckan-instance}/api/rest/{guid}
    #         * For a WAF record: http://{waf-root}/{file-name}
    #         * For a CSW record: http://{csw-server}/?Request=GetElementById&Id={guid}&...
    #
    #     :param harvest_object_id: HarvestObject id
    #     :returns: A string with the URL to the original document
    #     '''

    def gather_stage(self, harvest_job):
        '''
        The gather stage will receive a HarvestJob object and will be
        responsible for:
            - gathering all the necessary objects to fetch on a later.
              stage (e.g. for a CSW server, perform a GetRecords request)
            - creating the necessary HarvestObjects in the database, specifying
              the guid and a reference to its job. The HarvestObjects need a
              reference date with the last modified date for the resource, this
              may need to be set in a different stage depending on the type of
              source.
            - creating and storing any suitable HarvestGatherErrors that may
              occur.
            - returning a list with all the ids of the created HarvestObjects.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        :type harvest_job: HarvestJob
        '''

        log.debug('Entering gather_stage()')

        log.debug('Harvest source: {s}'.format(s=harvest_job.source.url))

        registry = importformats.create_metadata_registry()
        client = oaipmh.client.Client(harvest_job.source.url, registry)

        # Hot-patch HarvestJob to contain Client reference
        harvest_job.oaipmh_client = client

        # Choose best md_format from md_formats, but let's use 'oai_dc' for now
        md_formats = client.listMetadataFormats()
        md_format = 'oai_dc'

        # Hot-patch HarvestJob to contain MetadataPrefix reference
        harvest_job.oaipmh_md_format = md_format

        # Todo! Limit search to a specific Set
        set_ids = client.listSets()
        log.debug('listSets(): {s}'.format(s=set_ids))

        # Todo! Get all identifiers or records??
        package_ids = [header.identifier() for header in client.listIdentifiers(metadataPrefix=md_format)]
        # package_ids = [header.identifier() for header in client.listRecords()]
        log.debug('Identifiers: {i}'.format(i=package_ids))

        # Check if this source has been harvested before
        previous_job = Session.query(HarvestJob) \
            .filter(HarvestJob.source==harvest_job.source) \
            .filter(HarvestJob.gather_finished!=None) \
            .filter(HarvestJob.id!=harvest_job.id) \
            .order_by(HarvestJob.gather_finished.desc()) \
            .limit(1).first()

        if previous_job and not previous_job.gather_errors and not len(previous_job.objects) == 0:
            # Request only the packages modified since last harvest job
            last_time = previous_job.gather_finished.isoformat()
            # url = base_search_url + '/revision?since_time=%s' % last_time
            if False:
                self._save_gather_error('Unable to get content for: %s: %s' % (harvest_job.source.url, str(e)), harvest_job)

            if True:
                # for package_id in package_ids:
                #     if not package_id in package_ids:
                #         package_ids.append(package_id)
                pass
            else:
                log.info('No packages have been updated on the remote CKAN instance since the last harvest job')
                return None

        try:
            object_ids = []
            if len(package_ids):
                for package_id in package_ids:
                    # Create a new HarvestObject for this identifier
                    obj = HarvestObject(guid=package_id, job=harvest_job)
                    obj.save()
                    object_ids.append(obj.id)
                log.debug('Object ids: {i}'.format(i=object_ids))
                return object_ids
            else:
                self._save_gather_error('No packages received for URL: %s' % harvest_job.source.url, harvest_job)
                return None
        except Exception as e:
            self._save_gather_error('%r' % e.message, harvest_job)

        log.debug("Exiting gather_stage()")

    def fetch_stage(self, harvest_object):
        '''
        The fetch stage will receive a HarvestObject object and will be
        responsible for:
            - getting the contents of the remote object (e.g. for a CSW server,
              perform a GetRecordById request).
            - saving the content in the provided HarvestObject.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''

        log.debug("Entering fetch_stage()")
        log.debug("Exiting fetch_stage()")

        # Get source URL
        header, metadata, about = harvest_object.job.oaipmh_client.getRecord(
            identifier=harvest_object.id, metadataPrefix=harvest_object.job.oaipmh_md_format)

        # Get contents
        try:
            content = metadata  #.getMap()
        except Exception as e:
            self._save_object_error('Unable to get content for package: %s: %r' % (harvest_object.source.url, e), harvest_object)
            return None

        # Save the fetched contents in the HarvestObject
        harvest_object.content = content
        harvest_object.save()

        return True

    def import_stage(self, harvest_object):
        '''
        The import stage will receive a HarvestObject object and will be
        responsible for:
            - performing any necessary action with the fetched object (e.g
              create a CKAN package).
              Note: if this stage creates or updates a package, a reference
              to the package should be added to the HarvestObject.
            - creating the HarvestObject - Package relation (if necessary)
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''

        log.debug("Entering import_stage()")
        log.debug("Exiting import_stage()")

        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id, harvest_object, 'Import')
            return False

        package_dict = {
            'id': harvest_object.id,
            'title': 'my dataset title',
            'name': 'ckan-dataset-%s' % harvest_object.id,
            'notes': 'A long description of my dataset',
        }

        result = self._create_or_update_package(package_dict, harvest_object)

        return result
        # return True
