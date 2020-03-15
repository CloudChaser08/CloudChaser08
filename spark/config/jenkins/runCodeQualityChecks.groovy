#!/usr/bin/env groovy

/*
 * Jenkins pipeline for running code quality checks
 */


pipeline {

    agent any

    environment {
        STAGE = "test"

        // The lowest-allowable pylint score
        PYLINT_THRESHOLD="5.00"
    }

    stages {

        stage('Run python linting') {
            steps {
                sh """
                cd spark && make lint-python
                """
            }
        }
    }

    // Cleanup and notify
    post {

        cleanup {
            // Delete the workspace
            cleanWs();
        }
    }
}
