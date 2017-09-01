#!/bin/bash
set -euo pipefail

#
# Replaces the version defined in sources, usually x.y-SNAPSHOT,
# by a version identifying the build.
# The build version is composed of 4 fields, including the semantic version and
# the build number provided by Travis.
#
# Exported variables:
# - INITIAL_VERSION: version as defined in pom.xml
# - BUILD_VERSION: version including the build number
# - PROJECT_VERSION: target Maven version. The name of this variable is important because
#   it's used by QA when extracting version from Artifactory build info.
#
# Example of SNAPSHOT
# INITIAL_VERSION=6.3-SNAPSHOT
# BUILD_VERSION=6.3.0.12345
# PROJECT_VERSION=6.3.0.12345
#
# Example of RC
# INITIAL_VERSION=6.3-RC1
# BUILD_VERSION=6.3.0.12345
# PROJECT_VERSION=6.3-RC1
#
# Example of GA
# INITIAL_VERSION=6.3
# BUILD_VERSION=6.3.0.12345
# PROJECT_VERSION=6.3
#
result=0

function updateResult {
  result=$(($result + $?))
}

function fixBuildVersion {
  export INITIAL_VERSION=$(mvn -q \
    -Dexec.executable="echo" \
    -Dexec.args='${project.version}' \
    --non-recursive \
    org.codehaus.mojo:exec-maven-plugin:1.3.1:exec)

  # remove suffix -SNAPSHOT or -RC
  without_suffix=`echo $INITIAL_VERSION | sed "s/-.*//g"`

  IFS=$'.'
  fields_count=`echo $without_suffix | wc -w`
  unset IFS
  if [ $fields_count -lt 3 ]; then
    export BUILD_VERSION="$without_suffix.0.$TRAVIS_BUILD_NUMBER"
  else
    export BUILD_VERSION="$without_suffix.$TRAVIS_BUILD_NUMBER"
  fi

  if [[ "${INITIAL_VERSION}" == *"-SNAPSHOT" ]]; then
    # SNAPSHOT
    export PROJECT_VERSION=$BUILD_VERSION
    mvn org.codehaus.mojo:versions-maven-plugin:2.2:set -DnewVersion=$PROJECT_VERSION -DgenerateBackupPoms=false -B -e
  else
    # not a SNAPSHOT: milestone, RC or GA
    export PROJECT_VERSION=$INITIAL_VERSION
  fi

  echo "Build Version  : $BUILD_VERSION"
  echo "Project Version: $PROJECT_VERSION"
}
fixBuildVersion


export MAVEN_OPTS="-Xmx1G -Xms128m"
MAVEN_ARGS="-DbuildVersion=$BUILD_VERSION"

# Extract project group and name from pom.xml
base_project_key=$(mvn -q \
  -Dexec.executable="echo" \
  -Dexec.args='${project.groupId}:${project.artifactId}' \
  --non-recursive \
  org.codehaus.mojo:exec-maven-plugin:1.3.1:exec)
# Append branch name; for example: com.bakdata:ignite-hbase:master
project_key="$base_project_key:$TRAVIS_BRANCH"
# Now check if sonarcloud knows a project with the given key (may need to be extended for private repos)
code=$(curl -o /dev/null --silent --head --write-out '%{http_code}\n' \
  https://sonarcloud.io/api/project_analyses/search?project=$project_key)

# Some debug output
echo "HTTP response code $code for project key $project_key"
echo "Is pull request? $TRAVIS_PULL_REQUEST"
if [[ -n "${GITHUB_TOKEN:-}" ]]; then echo "Has github token"; else echo "No github token"; fi

if [[ "$TRAVIS_BRANCH" == "branch-"* ]] && [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
  echo 'Build release branch'

  mvn $MAVEN_ARGS clean deploy

  updateResult

elif [ "$code" == 404 ] && [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
  echo "Sonarcloud project with key $project_key not found. Skipping analysis.
  If you want to perform an analysis on that branch please add a corresponding sonarcloud project."

  mvn $MAVEN_ARGS clean install

  updateResult

elif [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
  echo "Build and analyze ${TRAVIS_BRANCH}"

  # Fetch all commit history so that SonarQube has exact blame information
  # for issue auto-assignment
  # This command can fail with "fatal: --unshallow on a complete repository does not make sense"
  # if there are not enough commits in the Git repository (even if Travis executed git clone --depth 50).
  # For this reason errors are ignored
  git fetch --unshallow 2> /dev/null || true

  incremental=$([ "$TRAVIS_BUILD_NUMBER" == *0 ] && echo "true" || echo "false")

  mvn $MAVEN_ARGS clean org.jacoco:jacoco-maven-plugin:prepare-agent install

  updateResult

  mvn $MAVEN_ARGS compile \
    sonar:sonar \
    -Dsonar.incremental=$incremental \
    -Dsonar.login=$SONAR_TOKEN \
    -Dsonar.projectVersion=$INITIAL_VERSION \
    -Dsonar.projectKey=$base_project_key \
    -Dsonar.branch=$TRAVIS_BRANCH \
    -Dsonar.junit.reportPaths=target/surefire-reports \
    -Dsonar.java.coveragePlugin=jacoco \
    -Dsonar.jacoco.reportPaths=target/jacoco.exec

  updateResult

elif [ "$TRAVIS_PULL_REQUEST" != "false" ] && [ -n "${GITHUB_TOKEN:-}" ] && [ "$code" != 404 ]; then
  echo 'Build and analyze internal pull request'

  mvn $MAVEN_ARGS clean org.jacoco:jacoco-maven-plugin:prepare-agent install

  updateResult

  mvn $MAVEN_ARGS compile \
    sonar:sonar \
    -Dsonar.analysis.mode=preview \
    -Dsonar.github.pullRequest=$TRAVIS_PULL_REQUEST \
    -Dsonar.github.repository=$TRAVIS_REPO_SLUG \
    -Dsonar.github.oauth=$GITHUB_TOKEN \
    -Dsonar.login=$SONAR_TOKEN \
    -Dsonar.projectKey=$base_project_key \
    -Dsonar.branch=$TRAVIS_BRANCH \
    -Dsonar.junit.reportPaths=target/surefire-reports \
    -Dsonar.java.coveragePlugin=jacoco \
    -Dsonar.jacoco.reportPaths=target/jacoco.exec

  updateResult

else
  echo "Build external pull request or pull request on branch with unknown key $project_key. Skipping analysis.
  If you want to perform an analysis on that branch please add a corresponding sonarcloud project."

  mvn $MAVEN_ARGS clean install

  updateResult
fi

exit $result
