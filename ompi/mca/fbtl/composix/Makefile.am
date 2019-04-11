#
# Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
#                         University Research and Technology
#                         Corporation.  All rights reserved.
# Copyright (c) 2004-2005 The University of Tennessee and The University
#                         of Tennessee Research Foundation.  All rights
#                         reserved.
# Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
#                         University of Stuttgart.  All rights reserved.
# Copyright (c) 2004-2005 The Regents of the University of California.
#                         All rights reserved.
# Copyright (c) 2008-2017 University of Houston. All rights reserved.
# Copyright (c) 2017      IBM Corporation.  All rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

# Make the output library in this directory, and name it either
# mca_<type>_<name>.la (for DSO builds) or libmca_<type>_<name>.la
# (for static builds).

if MCA_BUILD_ompi_fbtl_composix_DSO
component_noinst =
component_install = mca_fbtl_composix.la
else
component_noinst = libmca_fbtl_composix.la
component_install =
endif

mcacomponentdir = $(ompilibdir)
mcacomponent_LTLIBRARIES = $(component_install)
mca_fbtl_composix_la_SOURCES = $(sources)
mca_fbtl_composix_la_LDFLAGS = -module -avoid-version
mca_fbtl_composix_la_LIBADD = $(top_builddir)/ompi/lib@OMPI_LIBMPI_NAME@.la

noinst_LTLIBRARIES = $(component_noinst)
libmca_fbtl_composix_la_SOURCES = $(sources)
libmca_fbtl_composix_la_LDFLAGS = -module -avoid-version

# Source files

sources = \
        fbtl_composix.h \
        fbtl_composix.c \
        fbtl_composix_component.c \
        fbtl_composix_preadv.c \
        fbtl_composix_ipreadv.c \
        fbtl_composix_pwritev.c \
        fbtl_composix_ipwritev.c \
	fbtl_composix_lock.c \
	fbtl_composix_compression.c \
	fbtl_composix_debug.c \
	fbtl_composix_sz.c \
	fbtl_composix_sz_crc32.c 
