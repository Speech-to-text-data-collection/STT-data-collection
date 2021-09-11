from logging import log
import boto3
import uuid
from logger_creator import CreateLogger

logger = CreateLogger('AWSClient', handlers=1)
logger = logger.get_default_logger()


class AWSClient():
    def __init__(self) -> None:
        try:
            self.s3_client = boto3.client('s3')
            self.s3_resource = boto3.resource('s3')

            logger.info('AWSClient INSTANCE SUCCESSFULLY CREATED')

        except Exception as e:
            logger.exception("FAILED TO CREATE AWSClient INSTANCE")

    def generate_bucket_name(self, bucket_name: str) -> str:
        try:
            generated_name = ''.join([bucket_name, '_', str(uuid.uuid4())])
            
            logger.info(f'GENERATED UNIQUE BUCKET NAME: {generated_name}')
            return generated_name

        except Exception as e:
            logger.exception('FAILED TO GENERATE BUCKET NAME')

    def create_bucket(self, bucket_name: str, region_code: str = 'us-east-2'):
        # session = boto3.session.Session()
        # current_region = session.region_name
        # print(current_region)
        try:
            bucket_response = self.s3_resource.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': region_code}
            )

            logger.info(f'Created a new Bucket called: {bucket_name}')

            return (bucket_name, bucket_response)

        except Exception as e:
            logger.exception('FAILED TO CREATE NEW BUCKET')

    def delete_bucket(self, bucket_name: str):
        try:
            self.s3_resource.Bucket(bucket_name).delete()
        
            logger.info(f'SUCCESSFULLY DELETED BUCKET: {bucket_name}')

        except Exception as e:
            logger.exception('FAILED TO DELETE BUCKET: {bucket_name}')

    def get_file_names(self, bucket_name: str) -> list:
        try:
            bucket = self.s3_resource.Bucket(name=bucket_name)
            file_name_list = []
            for obj in bucket.objects.all():
                file_name_list.append(obj.key)

            logger.info(f'{bucket_name} BUCKETS FILE NAMES SUCCESSFULLY FETCHED')

            return file_name_list

        except Exception as e:
            logger.exception(f'FAILED TO RETRIEVE {bucket_name} BUCKETS FILE NAMES')

    def get_file_names_with_description(self, bucket_name: str) -> dict:
        try:

            bucket = self.s3_resource.Bucket(name=bucket_name)
            files_dict = {}
            for obj in bucket.objects.all():
                descriptions = obj.Object()
                files_dict[obj.key] = {'storage_class': obj.storage_class,
                                    'last_modified': obj.last_modified,
                                    'version_id': descriptions.version_id,
                                    'meta_data': descriptions.metadata
                                    }

            logger.info(f'{bucket_name} BUCKETS FILE NAMES SUCCESSFULLY FETCHED WITH DESCRIPTIONS')

            return files_dict

        except Exception as e:
            logger.exception(f'FAILED TO RETRIEVE {bucket_name} BUCKETS FILE NAMES WITH DESCRIPTIONS')


    def upload_file(self, bucket_name: str, file_name: str, key: str) -> None:
        try:
            self.s3_resource.Object(bucket_name,
                                key).upload_file(Filename=file_name)

            logger.info(f'{file_name} UPLOADED SUCCESSFULLY TO BUCKET {bucket_name} as {key}')
        
        except Exception as e:
            logger.exception('FAILED TO UPLOAD FILE')

    def upload_file_bytes(self, data, bucket_name: str, file_name: str):
        try:
            data = data.encode()
            self.s3_client.put_object(Body=data,
                                  Bucket=bucket_name, Key=file_name)

            logger.info(f'UPLOADED BYTES SUCCESSFULLY TO BUCKET {bucket_name} as {file_name}')

        except Exception as e:
            logger.exception('FAILED TO UPLOAD BYTES')

    def get_file_link(self, bucket_name: str, file_name: str) -> str:
        try:
            bucket_location = self.s3_client.get_bucket_location(
                Bucket=bucket_name)
            object_url = "https://s3-{0}.amazonaws.com/{1}/{2}".format(
                bucket_location['LocationConstraint'],
                bucket_name,
                file_name)

            logger.info(f'SUCCESSFULLY GENERATED FILE LINK')

            return object_url

        except Exception as e:
            logger.exception('FAILED TO GENERATE FILE LINK')

    def load_file_bytes(self, bucket_name: str, file_name: str):
        try:
            s3_object = self.s3_client.get_object(
            Bucket=bucket_name, Key=file_name)
            data = s3_object['Body'].read()

            logger.info(f'SUCCESSFULLY LOADED FILE {file_name} AS BYTES')

            return data

        except Exception as e:
            logger.exception('FAILED TO LOAD FILE BYTES')

    def download_file(self, bucket_name: str, file_name: str, location: str = '/tmp/'):
        try:
            self.s3_resource.Object(bucket_name, file_name).download_file(
            f'{location}{file_name}')

            logger.info(f'{file_name} SUCCESSFULLY DOWNLOAD TO {location}')
        
        except Exception as e:
            logger.exception('FAILED TO DOWNLOAD FILE')

    def delete_file(self, bucket_name: str, file_name: str):
        try:
            self.s3_resource.Object(bucket_name, file_name).delete()

            logger.info(f'{file_name} SUCCESSFULLY DELETED FROM {bucket_name} BUCKET')

        except Exception as e:
            logger.exception('FAILED TO DELETE FILE FROM BUCKET')


    def delete_all_files_From_bucket(self, bucket_name: str):
        try:
            res = []
            bucket = self.s3_resource.Bucket(bucket_name)
            for obj_version in bucket.object_versions.all():
                res.append({'Key': obj_version.object_key,
                            'VersionId': obj_version.id})

            bucket.delete_objects(Delete={'Objects': res})

            logger.info(f'SUCCESSFULLY DELETED ALL FILES IN {bucket_name} BUCKET')

        except Exception as e:
            logger.exception(f'FAILED TO DELETE ALL FILES IN {bucket_name} BUCKET')
