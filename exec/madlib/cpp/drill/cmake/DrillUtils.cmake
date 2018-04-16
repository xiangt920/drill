

# Add the installer group for this port and a component for all files that are
# not version-specific
#
# We dynamically generate a CMake input file here. This is because the effects
# of cpack_add_component_group() are not globally visible. Hence, we generate
# a file in the deploy directory that CMake will execute only at the very end.
# The motivation is that that way we want to have a clean separation between
# port-specific source code and general code.
#
function(cpack_add_port_group_and_component_for_all_versions)
  file(WRITE "${PORT_DEPLOY_SCRIPT}" "
    cpack_add_component_group(${PORT}
        DISPLAY_NAME \"${PORT} Support\"
        DESCRIPTION \"MADlib support for ${PORT}.\"
        PARENT_GROUP ports
    )
    cpack_add_component(${PORT}_any
        DISPLAY_NAME \"All Versions\"
        DESCRIPTION \"MADlib files shared by all ${PORT} versions.\"
        GROUP ${PORT}
    )")
endfunction(cpack_add_port_group_and_component_for_all_versions)


# Add the installer component for version-specific files
#
function(cpack_add_version_component)
  file(APPEND "${PORT_DEPLOY_SCRIPT}" "
    cpack_add_component(${DBMS}
        DISPLAY_NAME \"${IN_PORT_VERSION}\"
        DESCRIPTION \"MADlib files specific to ${PORT} ${IN_PORT_VERSION}.\"
        GROUP ${PORT}
    )")
endfunction(cpack_add_version_component)


# Determine the versions of this port that we need to build for.
#
# If the user specifies at least one ${PORT_UC}_X_Y_PG_CONFIG, we only build
# for that specific version. If no such variable is defined, we look for any
# version of this port. This function will have a *side effect* in that case:
# It sets one ${PORT_UC}_X_Y_PG_CONFIG to the path to pg_config that was found.
#
function(determine_target_versions OUT_VERSIONS)
  get_subdirectories("${CMAKE_CURRENT_SOURCE_DIR}" SUPPORTED_VERSIONS)
  get_filtered_list(SUPPORTED_VERSIONS "^[0-9]+.*$" ${SUPPORTED_VERSIONS})

  # Pass values to caller
  set(${OUT_VERSIONS} "${${OUT_VERSIONS}}" PARENT_SCOPE)
  # ${PORT_UC}_${_VERSION_UNDERSCORE}_PG_CONFIG might have been set earlier!
  # (the side effect)
endfunction(determine_target_versions)
