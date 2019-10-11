#!groovy
@Library('juiceinc-library') _

pipeline {
  agent  { label 'python-ecs' }
  stages {
    stage('Starting') {
      steps {
        sendNotifications 'STARTED'
      }
    }
    stage('Linting') {
      steps {
        retry(count: 3) {
          sh '''
#!/usr/bin/bash
set -ex
VENV=".venv"
python3.6 -m venv $VENV
. "$VENV/bin/activate"
pip3.6 install -qq -r requirements_dev.txt
echo "flake8">> flake8_errors.txt
flake8 --output-file=flake8_errors.txt --exit-zero .'''
        }

      }
    }
    stage('Testing') {
      steps {
        retry(count: 3) {
          sh '''
#!/usr/bin/bash
VENV=".venv"
. "$VENV/bin/activate"
py.test --cov-config .coveragerc --cov recipe --cov-report term-missing --cov-report xml --junitxml junit.xml
'''
        }
      }
    }
  }
  post {
    always {
      archiveArtifacts '**/flake8_errors.txt, **/junit.xml, **/coverage.xml'
      warnings canComputeNew: false, canResolveRelativePaths: false, canRunOnFailed: true, categoriesPattern: '', defaultEncoding: '', excludePattern: '', failedTotalAll: '10', failedTotalHigh: '1', failedTotalLow: '0', failedTotalNormal: '0', healthy: '0', includePattern: '', messagesPattern: '', parserConfigurations: [[parserName: 'Pep8', pattern: 'flake8_errors.txt']], unHealthy: '1', unstableTotalAll: '10', unstableTotalHigh: '0', unstableTotalLow: '0', unstableTotalNormal: '1'
      junit 'junit.xml'
      step([$class: 'CoberturaPublisher', autoUpdateHealth: false, autoUpdateStability: false, coberturaReportFile: '**/coverage.xml', failUnhealthy: false, failUnstable: false, maxNumberOfBuilds: 0, onlyStable: false, sourceEncoding: 'ASCII', zoomCoverageChart: false])
      sendNotifications currentBuild.result
    }
  }
}