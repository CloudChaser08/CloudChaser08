/* Build and deploy the Spark library of routines (normalization, state, etc)
 * from the Dewey repository to the Analytics Cluster for use in Zeppelin.
 *
 * WARNING: Running this will force Zeppelin users to restart their Spark
 * interpreters. Announce in the #zepp channel before you run this.
 *
 * ONLY RUN: Tuesday - Thursday
 */

pipeline {

    agent { node { label 'ubuntu-18.04-worker' } }

    environment {
        SLACK_CHANNEL = 'CQFP972J3'

        // Namespaces docker-compose to not compete with concurrent builds
        COMPOSE_PROJECT_NAME = "dewey-zeppelin-deploy-${env.BRANCH_NAME}-${currentBuild.id}"

        // AWS Settings
        AWS_DEFAULT_REGION = 'us-east-1'
        AWS_ACCESS_KEY_ID = sh(script: "curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/jenkins_worker | jq -r '.AccessKeyId'", , returnStdout: true).trim()
        AWS_SECRET_ACCESS_KEY = sh(script: "curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/jenkins_worker | jq -r '.SecretAccessKey'", , returnStdout: true).trim()
        AWS_SESSION_TOKEN = sh(script: "curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/jenkins_worker | jq -r '.Token'", , returnStdout: true).trim()
    }

    stages {

        stage('Build') {
            steps {
                sh 'make package-spark'
            }
        }

        stage('Deploy to analytics cluster') {
            steps {
                sh '''
                aws ssm get-parameter --name /prod/ssh/analytics/emr_analytics_20200702 --with-decryption | jq -r '.Parameter.Value' > emr_deployer
                export KEY_FILE=emr_deployer
                cd spark && make analytics-deploy
                '''
            }
        }
    }

    post {

        success {
            slackSend (color: '#00FF00',
                message: "SUCCESSFUL\nJob: '${env.BUILD_TAG}'\nView build: (${env.BUILD_URL})",
                channel: "${SLACK_CHANNEL}")
        }

        failure {
            slackSend (color: '#FF0000',
                message: "FAILED\nJob: '${env.BUILD_TAG}'\nView build: (${env.BUILD_URL})",
                channel: "${SLACK_CHANNEL}")
        }

        aborted {
            slackSend (color: '#FF9500',
                message: "ABORTED\nJob: '${env.BUILD_TAG}'\nView build: (${env.BUILD_URL})",
                channel: "${SLACK_CHANNEL}")
        }

        cleanup {
            cleanWs();
        }
    }
}
