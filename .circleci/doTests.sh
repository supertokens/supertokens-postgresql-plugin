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
	\"planType\":\"COMMERCIAL\",
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
while read line
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
    "https://api.supertokens.io/0/plugin-interface/dependency/core/latest?password=$SUPERTOKENS_API_KEY&planType=COMMERCIAL&mode=DEV&version=$piVersion" \
    -H 'api-version: 0'`
    if [[ `echo $response | jq .core` == "null" ]]
    then
        echo "fetching latest X.Y version for core given plugin-interface X.Y version: $piVersion gave response: $response"
        exit 1
    fi
    coreVersionX2=$(echo $response | jq .core | tr -d '"')
    
    response=`curl -s -X GET \
    "https://api.supertokens.io/0/core/latest?password=$SUPERTOKENS_API_KEY&planType=COMMERCIAL&mode=DEV&version=$coreVersionX2" \
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
    pluginX=$(cut -d'.' -f1 <<<"$pluginVersion")
    pluginY=$(cut -d'.' -f2 <<<"$pluginVersion")
    echo -e "core,$coreVersionX2\nplugin-interface,$piVersion\npostgresql-plugin,$pluginX.$pluginY" > modules.txt
    ./loadModules
    cd supertokens-core
    git checkout $coreVersionTag
    cd ../supertokens-plugin-interface
    git checkout $currTag
    cd ../supertokens-postgresql-plugin
    git checkout dev-v$pluginVersion
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
done <<< `cat pluginInterfaceExactVersionsOutput`

if [[ $someTestsRan = "true" ]]
then
    echo "calling /core PATCH to make testing passed"
    responseStatus=`curl -s -o /dev/null -w "%{http_code}" -X PATCH \
        https://api.supertokens.io/0/plugin \
        -H 'Content-Type: application/json' \
        -H 'api-version: 0' \
        -d "{
            \"password\": \"$SUPERTOKENS_API_KEY\",
            \"planType\":\"COMMERCIAL\",
            \"name\":\"postgresql\",
            \"version\":\"$pluginVersion\",
            \"testPassed\": true
        }"`
    if [ $responseStatus -ne "200" ]
    then
        echo "patch api failed"
        exit 1
    fi
else
    echo "no test ran"
    exit 1
fi