function cleanup {
    if test -f "pluginInterfaceExactVersionsOutput"; then
        rm pluginInterfaceExactVersionsOutput 
    fi
}

trap cleanup EXIT
cleanup

pluginInterfaceJson=`cat ../pluginInterfaceSupported.json`
pluginInterfaceLength=`echo $pluginInterfaceJson | jq ".versions | length"`
pluginInterfaceArray=`echo $pluginInterfaceJson | jq ".versions"`
echo "got plugin interface relations"

./getPluginInterfaceExactVersions.sh $pluginInterfaceLength "$pluginInterfaceArray"

if [[ $? -ne 0 ]]
then
    echo "all plugin interfaces found... failed. exiting!"
	exit 1
else
    echo "all plugin interfaces found..."
fi

# get plugin version
pluginVersion=`cat ../build.gradle | grep -e "version =" -e "version="`
while IFS='"' read -ra ADDR; do
    counter=0
    for i in "${ADDR[@]}"; do
        if [ $counter == 1 ]
        then
            pluginVersion=$i
        fi
        counter=$(($counter+1))
    done
done <<< "$pluginVersion"

responseStatus=`curl -s -o /dev/null -w "%{http_code}" -X PUT \
  https://api.supertokens.io/0/plugin \
  -H 'Content-Type: application/json' \
  -H 'api-version: 0' \
  -d "{
	\"password\": \"$SUPERTOKENS_API_KEY\",
	\"planType\":\"FREE\",
	\"version\":\"$pluginVersion\",
	\"pluginInterfaces\": $pluginInterfaceArray,
	\"name\": \"postgresql\"
}"`
if [ $responseStatus -ne "200" ]
then
    echo "failed plugin PUT API status code: $responseStatus. Exiting!"
	exit 1
fi

someTestsRan=false
while read -u 10 line
do
    if [[ $line = "" ]]; then
        continue
    fi
    i=0
    currTag=`echo $line | jq .tag`
    currTag=`echo $currTag | tr -d '"'`

    currVersion=`echo $line | jq .version`
    currVersion=`echo $currVersion | tr -d '"'`
    piX=$(cut -d'.' -f1 <<<"$currVersion")
    piY=$(cut -d'.' -f2 <<<"$currVersion")
    piVersion="$piX.$piY"
    
    someTestsRan=true
    
    response=`curl -s -X GET \
    "https://api.supertokens.io/0/plugin-interface/dependency/core/latest?password=$SUPERTOKENS_API_KEY&planType=FREE&mode=DEV&version=$piVersion" \
    -H 'api-version: 0'`
    if [[ `echo $response | jq .core` == "null" ]]
    then
        echo "fetching latest X.Y version for core given plugin-interface X.Y version: $piVersion gave response: $response"
        exit 1
    fi
    coreVersionX2=$(echo $response | jq .core | tr -d '"')
    
    response=`curl -s -X GET \
    "https://api.supertokens.io/0/core/latest?password=$SUPERTOKENS_API_KEY&planType=FREE&mode=DEV&version=$coreVersionX2" \
    -H 'api-version: 0'`
    if [[ `echo $response | jq .tag` == "null" ]]
    then
        echo "fetching latest X.Y.Z version for core X.Y version: $coreVersionX2 gave response: $response"
        exit 1
    fi
    coreVersionTag=$(echo $response | jq .tag | tr -d '"')

    cd ../../
    git clone git@github.com:supertokens/supertokens-root.git
    cd supertokens-root
    rm gradle.properties

    update-alternatives --install "/usr/bin/java" "java" "/usr/java/jdk-15.0.1/bin/java" 2
    update-alternatives --install "/usr/bin/javac" "javac" "/usr/java/jdk-15.0.1/bin/javac" 2

    pluginX=$(cut -d'.' -f1 <<<"$pluginVersion")
    pluginY=$(cut -d'.' -f2 <<<"$pluginVersion")
    echo -e "core,$coreVersionX2\nplugin-interface,$piVersion\npostgresql-plugin,$pluginX.$pluginY" > modules.txt
    ./loadModules

    #gradle properties overrides
    export _JAVA_OPTIONS="-Xms1g -Xmx4g"
    echo -e "org.gradle.jvmargs=-Xms1g -Xmx4g -XX:+UseContainerSupport" > gradle.properties
    echo -e "org.gradle.daemon=false" >> gradle.properties
    #echo -e "org.gradle.logging.level=debug" >> gradle.properties # remove this after discovering the issue


    cd supertokens-core
    git checkout test-cicd/mysql8-upgrade
    cd ../supertokens-plugin-interface
    git checkout $currTag
    cd ../supertokens-postgresql-plugin
    #git checkout dev-v$pluginVersion
    git checkout "test-cicd/upgrade-postgresql-to-13"
    cd ../
    echo $SUPERTOKENS_API_KEY > apiPassword
    ./startTestingEnv --cicd

    if [[ $? -ne 0 ]]
    then
        cat logs/*
        cd ../project/
        echo "test failed... exiting!"
        exit 1
    fi
    cd ../
    rm -rf supertokens-root
    cd project/.circleci
done 10<pluginInterfaceExactVersionsOutput

if [[ $someTestsRan = "true" ]]
then
    echo "all tests ran"
    exit 0
else
    echo "no test ran"
    exit 1
fi