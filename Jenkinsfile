pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
	       sh '/opt/anaconda3/bin/pipenv install --dev'
               sh '/opt/anaconda3/bin/pipenv --python python3 sync'
            }
        }
        stage('Test') {
            steps {
               sh '/opt/anaconda3/bin/pipenv run pytest'
            }
        }
        stage('Package') {
	    when{
		    anyOf{ branch "main" ; branch 'release' }
	    }
            steps {
               sh 'zip -r lib.zip lib'
            }
        }
	stage('Release') {
	   when{
	      branch 'release'
	   }
           steps {
              sh 'cp lib.zip log4j.properties project_main.py spark_project_submit.sh conf /Users/dzik/PycharmProjects/qa/'
           }
        }
	stage('Deploy') {
	   when{
	      branch 'main'
	   }
           steps {
               sh 'cp lib.zip log4j.properties project_main.py spark_project_submit.sh conf /Users/dzik/PycharmProjects/prod/'
           }
        }
    }
}
