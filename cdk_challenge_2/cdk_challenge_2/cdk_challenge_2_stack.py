from aws_cdk import (
    # Duration,
    Stack,
    # aws_sqs as sqs,
)
from constructs import Construct

from os import path
from aws_cdk import (
    aws_apigateway as _agw,
    aws_lambda as _lambda,
    aws_iam as _iam,
    aws_ec2 as ec2)

from .stacks import (

    lambda_stack
)

import aws_cdk as core
from constructs import Construct


class CdkChallenge2Stack(Stack):

    def __init__(self, scope: Construct, construct_id: str, stage="dev_values", **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

      
        # Get variables by stage
        shared_values = self.get_variables(self)
 

        # Create the Bucket
        bucket_name = "student-metrics"
        lambda_role = _iam.Role.from_role_arn(self, 'student_role', role_arn=shared_values['rol_arn'])
        
        lambda_layer = _lambda.LayerVersion.from_layer_version_attributes(self, 'student_layer', layer_version_arn=shared_values['layer_arn'])
        this_dir = path.dirname(__file__)




        # The code that defines your stack goes here

        # example resource
        # queue = sqs.Queue(
        #     self, "CdkChallenge2Queue",
        #     visibility_timeout=Duration.seconds(300),
        # )
                # Create Lambdas
        ConsultDbGlobant_1 = lambda_stack.lambdaStack(self, 'ConsultDbGlobant_1', lambda_name='ConsultDbGlobant',shared_values=shared_values, has_security=True, has_mongo=False)
   
        ConsultDbGlobant_2 = lambda_stack.lambdaStack(self, 'ConsultDbGlobant_2', lambda_name='ConsultDbGlobant2',shared_values=shared_values, has_security=True, has_mongo=False)
        get_json_to_db = lambda_stack.lambdaStack(self, 'get_json_to_db', lambda_name='get_json_to_db',shared_values=shared_values, has_security=True, has_mongo=False)
         
           #APIs
        api_name="cdk_globant_challenge"

        api = _agw.RestApi(
            self,
            api_name,
            description='API for users hired globant',
            deploy=False
            )
        # Main Resources
        user_resource = api.root.add_resource("employees_hired")

        # Main Resources
        insert_information = api.root.add_resource("insert_information")


        # Paths resources
        most_popular_resource = user_resource.add_resource("job_department")
        course_month_resource = user_resource.add_resource("count_each_department")
        
        insert_information_resource = insert_information.add_resource("{id_table}")
      

        # Integrate API and courseMonth lambda
        course_month_integration = _agw.LambdaIntegration(ConsultDbGlobant_1.student_lambda)
        course_month_integration_2 = _agw.LambdaIntegration(ConsultDbGlobant_2.student_lambda)

        insert_information_integration = _agw.LambdaIntegration(get_json_to_db.student_lambda)



        course_month_resource.add_method(
            "GET",
            course_month_integration
        )

        
        most_popular_resource.add_method(
            "GET",
            course_month_integration_2,
        )

        insert_information_resource.add_method(
            "POST",
            insert_information_integration,
        )
    @staticmethod
    def get_variables(self):
        shared_values = self.node.try_get_context('shared_values') 
        return shared_values['dev_values']

