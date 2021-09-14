from logging import log
import boto3
import uuid
from tempfile import SpooledTemporaryFile
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

            logger.info(
                f'{bucket_name} BUCKETS FILE NAMES SUCCESSFULLY FETCHED')

            return file_name_list

        except Exception as e:
            logger.exception(
                f'FAILED TO RETRIEVE {bucket_name} BUCKETS FILE NAMES')

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

            logger.info(
                f'{bucket_name} BUCKETS FILE NAMES SUCCESSFULLY FETCHED WITH DESCRIPTIONS')

            return files_dict

        except Exception as e:
            logger.exception(
                f'FAILED TO RETRIEVE {bucket_name} BUCKETS FILE NAMES WITH DESCRIPTIONS')

    def upload_file(self, bucket_name: str, file_name: str, key: str) -> None:
        try:
            self.s3_resource.Object(bucket_name,
                                    key).upload_file(Filename=file_name, ExtraArgs={'ACL': 'public-read'})

            logger.info(
                f'{file_name} UPLOADED SUCCESSFULLY TO BUCKET {bucket_name} as {key}')

        except Exception as e:
            logger.exception('FAILED TO UPLOAD FILE')
            raise Exception

    def put_file(self, bucket_name: str, file_contents, key: str) -> None:
        try:
            self.s3_resource.Object(bucket_name,
                                    key, ExtraArgs={'ACL': 'public-read'}).put(Body=file_contents)

            logger.info(
                f'{key} PUT SUCCESSFULLY TO BUCKET {bucket_name} USING BODY DATA')

        except Exception as e:
            logger.exception('FAILED TO PUT FILE')
            raise Exception

    def upload_file_bytes(self, data, bucket_name: str, file_name: str, encode: bool = False):
        try:
            if encode:
                data = data.encode()
            self.s3_client.put_object(Body=data,
                                      Bucket=bucket_name, Key=file_name, ExtraArgs={'ACL': 'public-read'})

            logger.info(
                f'UPLOADED BYTES SUCCESSFULLY TO BUCKET {bucket_name} as {file_name}')

        except Exception as e:
            logger.exception('FAILED TO UPLOAD BYTES')

    def upload_file_object(self, temp_file: SpooledTemporaryFile, bucket_name: str, file_name: str):
        try:
            self.s3_client.upload_fileobj(
                Fileobj=temp_file, Bucket=bucket_name, Key=file_name, ExtraArgs={'ACL': 'public-read'})

            logger.info(
                f'UPLOADED FILE OBJECT SUCCESSFULLY TO BUCKET {bucket_name} as {file_name}')

        except Exception as e:
            logger.exception('FAILED TO UPLOAD FILE OBJECT')

    def get_file_link(self, bucket_name: str, file_name: str) -> str:
        try:
            bucket_location = self.s3_client.get_bucket_location(
                Bucket=bucket_name)
            object_url = "https://{0}.s3.{1}.amazonaws.com/{2}".format(
                bucket_name,
                bucket_location['LocationConstraint'],
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

            logger.info(
                f'{file_name} SUCCESSFULLY DELETED FROM {bucket_name} BUCKET')

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

            logger.info(
                f'SUCCESSFULLY DELETED ALL FILES IN {bucket_name} BUCKET')

        except Exception as e:
            logger.exception(
                f'FAILED TO DELETE ALL FILES IN {bucket_name} BUCKET')
