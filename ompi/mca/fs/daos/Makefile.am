#
# Copyright (c) 2019      University of Houston. All rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

# Make the output library in this directory, and name it either
# mca_<type>_<name>.la (for DSO builds) or libmca_<type>_<name>.la
# (for static builds).

if MCA_BUILD_ompi_fs_daos_DSO
component_noinst =
component_install = mca_fs_daos.la
else
component_noinst = libmca_fs_daos.la
component_install =
endif

mcacomponentdir = $(ompilibdir)
mcacomponent_LTLIBRARIES = $(component_install)
mca_fs_daos_la_SOURCES = $(sources)
mca_fs_daos_la_LDFLAGS = -module -avoid-version
mca_fs_daos_la_LIBADD = $(top_builddir)/ompi/lib@OMPI_LIBMPI_NAME@.la

noinst_LTLIBRARIES = $(component_noinst)
libmca_fs_daos_la_SOURCES = $(sources)
libmca_fs_daos_la_LDFLAGS = -module -avoid-version

# Source files

sources = \
        fs_daos.h \
        fs_daos.c \
        fs_daos_component.c \
        fs_daos_file_open.c 
