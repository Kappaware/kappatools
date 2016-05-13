#!/bin/bash

# Thanks to https://discuss.gradle.org/t/how-can-i-provide-command-line-args-to-application-started-with-gradle-run/6474/6


#Join arguments in a string. But not quite like you'd expect... see next line.
printf -v var "'%s', " "$@"

#Remove trailing ", "
var=${var%??}

gradle run_main -PrunArgs="[$var]"

