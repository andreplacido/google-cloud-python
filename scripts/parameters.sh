
#!/bin/bash

# Loop through arguments and process them

 for arg in "$@"

 do

 #echo $arg



case $arg in

 -f|--feedname)

 FEED_NAME="$2"

 shift # Remove --feedname argument from processing

 ;;

 *)

 OTHER_ARGUMENTS+=("$1")

 shift # Remove generic argument from processing

 ;;

 esac

 done

echo $FEED_NAME


