# -*- shell-script -*-
#
# Copyright (c) 2012-2015 NVIDIA Corporation.  All rights reserved.
# Copyright (c) 2015      Los Alamos National Security, LLC. All rights
#                         reserved.
# Copyright (c) 2023      Advanced Micro Devices, Inc. All Rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

#
# If ROCM support was requested, then build the ROCM memory pools.
# This code checks the variable ROCM_SUPPORT which was set earlier in
# the configure sequence by the opal_configure_options.m4 code.
#

AC_DEFUN([MCA_opal_rcache_rgpurocsm_CONFIG],[
    AC_CONFIG_FILES([opal/mca/rcache/rgpurocsm/Makefile])

    OPAL_CHECK_ROCM([rcache_rgpurocsm])

    # Use ROCM_SUPPORT which was filled in by the opal configure code.
    AS_IF([test "x$ROCM_SUPPORT" = "x1"],
          [$1],
          [$2])

    AC_SUBST([rcache_rgpurocsm_CPPFLAGS])
    AC_SUBST([rcache_rgpurocsm_LDFLAGS])
    AC_SUBST([rcache_rgpurocsm_LIBS])
])dnl
