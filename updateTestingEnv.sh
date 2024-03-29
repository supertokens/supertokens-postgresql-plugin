exitIfNeeded () {
  if [ $? -ne 0 ]
	then
		exit 1
	fi
}

prefix=`(cd ../ && ./utils/project-prefix)`
(cd ../ && ./gradlew :$prefix-postgresql-plugin:clean < /dev/null)

exitIfNeeded

(cd ../ && ./gradlew :$prefix-postgresql-plugin:build -x test < /dev/null)

exitIfNeeded

cp ./build/libs/* ../plugin
