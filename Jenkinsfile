import groovy.json.JsonSlurper
import java.util.HashMap

pipeline {
    agent any

    environment {
        REPO_URL = 'https://github.com/ganasai88/ETL-Spark.git' // GitHub repository URL4
        VENV_DIR = 'venv'
        S3_DIR = 'S3-Terraform'
        EMR_DIR = 'EMR'
        ACCESS_KEY = credentials('AWS_ACCESS_KEY_ID')
        SECRET_KEY = credentials('AWS_SECRET_KEY_ID')
        REGION = 'us-east-2'
        S3_BUCKET = 'etlspark'
        STEP_NAME = 'Run Monthly Spark Job'
    }

    stages {
        stage('Checkout') {
            steps {

                git branch: 'main', url: "${REPO_URL}"

            }
        }
        stage('SonarQube Analysis') {
                     steps {
                         script {
                                 sh '''
                                 docker run --rm -e SONAR_HOST_URL=http://18.117.71.55:9000/ \
                                 -e SONAR_LOGIN=sqp_8c2e771f33cbe36bb53dc30cb568db790a2736c7 \
                                 -v /var/lib/jenkins/workspace/Spark-Multi:/usr/src \
                                 sonarsource/sonar-scanner-cli \
                                 -Dsonar.projectKey=ETL-Pyspark \
                                 -Dsonar.sources=. \
                                 -Dsonar.host.url=http://18.117.71.55:9000/ \
                                 -Dsonar.login=sqp_8c2e771f33cbe36bb53dc30cb568db790a2736c7 \
                                 '''
                         }
                     }
        }
        stage('Upload Files to S3') {
            steps {
                 script {
                        sh '''
                            # Change to the directory containing the .py files
                            cd monthly/22-11-2024

                            # Zip the .py files and overwrite if exists
                            zip -jo py_files_22-11-2024.zip *.py
                            echo "Zip file created successfully: py_files_22-11-2024.zip"

                            # Upload everything to S3 bucket, including updated zip and other files, ensuring overwrite
                            echo "Uploading files to S3 bucket ${S3_BUCKET}..."

                            # Sync the code to S3, include all files and exclude .git
                            aws s3 sync . s3://${S3_BUCKET}/monthly/22-11-2024/ --exact-timestamps --exclude ".git/*"

                            echo "Files uploaded successfully!"
                        '''
                 }
            }
        }
        stage('Find Running EMR Cluster') {
            steps {
                script {
                    // Get the cluster ID of the first running EMR cluster
                    def clusterId = sh(
                        script: '''
                        aws emr list-clusters \
                            --active \
                            --query "Clusters[?Status.State=='WAITING']|[0].Id" \
                            --region $REGION \
                            --output text
                        ''',
                        returnStdout: true
                    ).trim()
                    if (!clusterId) {
                        error "No running EMR cluster found!"
                    }
                    echo "Found EMR Cluster ID: ${clusterId}"
                    env.CLUSTER_ID = clusterId
                }
            }
        }
        stage('Add Step to EMR Cluster') {
            steps {
                script {
                   // Adding step to the running EMR cluster
                   def addStepCommand = """
                       aws emr add-steps \
                           --cluster-id ${env.CLUSTER_ID} \
                           --steps '[{
                               "Type": "Spark",
                               "Name": "${STEP_NAME}",
                               "ActionOnFailure": "CONTINUE",
                               "Args": [
                                   "--deploy-mode", "cluster",
                                   "--py-files","s3://${S3_BUCKET}/monthly/22-11-2024/py_files_22-11-2024.zip",
                                   "s3://${S3_BUCKET}/monthly/22-11-2024/main.py",
                                   "--json_file_path", "s3a://${S3_BUCKET}/monthly/22-11-2024/configurations/config.json"
                               ]
                           }]' \
                           --region ${REGION}
                   """
                   // Execute the add-steps command and capture the output
                   def result = sh(script: addStepCommand, returnStdout: true).trim()

                   // Log the output for debugging
                   echo "Add step result: ${result}"

                   // Parse the JSON output using Groovy's JsonSlurper
                   def jsonResponse = new groovy.json.JsonSlurper().parseText(result)

                   // Extract the step ID
                   def stepId = jsonResponse.StepIds[0] // Extract the first step-id from the StepIds array

                   if (stepId) {
                       // Save the timestamp and step ID for later use
                       def timestamp = new Date().format('yyyy-MM-dd HH:mm:ss')
                       currentBuild.description = "Step added at: ${timestamp} with Step ID: ${stepId}"

                       // Store the stepId as an environment variable for use in the next stage
                       env.STEP_ID = stepId
                       env.TIMESTAMP = timestamp

                       echo "Step added to EMR Cluster ID: ${env.CLUSTER_ID} at ${timestamp}, Step ID: ${stepId}"
                   } else {
                        error "Step ID not found in the output"
                    }
                }
            }
        }
        stage('Step Completed Successfully') {
            steps {
                script {
                    // Use the saved step ID from the previous stage
                    echo "Retrieving status for Step ID: ${env.STEP_ID}"

                    // Command to get step status using the saved Step ID
                    def getStepStatusCommand = """
                        aws emr describe-step \
                            --cluster-id ${env.CLUSTER_ID} \
                            --step-id ${env.STEP_ID} \
                            --region ${REGION}
                    """

                    // Loop to continuously check the step status
                    def stepStatus = ""
                    def isStepCompleted = false

                    while (!isStepCompleted) {
                        // Execute the command and capture the output
                        def statusResult = sh(script: getStepStatusCommand, returnStdout: true).trim()
                        echo "Step status result: ${statusResult}"

                        // Call the method that processes the result in a non-CPS context
                        stepStatus = getStepState(statusResult)

                        // Check if the step has completed or failed
                        if (stepStatus == 'COMPLETED') {
                            echo "Step completed successfully."
                            isStepCompleted = true
                        } else if (stepStatus == 'FAILED') {
                            echo "Step failed."
                            isStepCompleted = true
                            error "EMR step failed with Step ID: ${env.STEP_ID}"
                        } else {
                            echo "Step is still in state: ${stepStatus}. Waiting for completion..."
                            // Wait before checking again
                            sleep(30) // Wait for 30 seconds before rechecking
                        }
                    }
                }
            }
        }
    }
}
// Non-CPS method to handle JSON parsing and state extraction
@NonCPS
def getStepState(String statusResult) {
    // Parse the JSON result
    def jsonResponse = new JsonSlurper().parseText(statusResult)

    // Convert LazyMap to HashMap to avoid serialization issues
    def serializableResponse = new HashMap(jsonResponse)

    // Get the step state from the serializable response
    return serializableResponse.Step.Status.State
}

