# -*- shell-script -*-
#
# Copyright (c) 2009-2013 The University of Tennessee and The University
#                         of Tennessee Research Foundation.  All rights
#                         reserved.
# Copyright (c) 2009-2010 Cisco Systems, Inc.  All rights reserved.
# Copyright (c) 2012-2015 NVIDIA Corporation.  All rights reserved.
# Copyright (c) 2022      Amazon.com, Inc. or its affiliates.  All Rights reserved.
# Copyright (c) 2023      Advanced Micro Devices, Inc. All Rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

# MCA_btl_smrocm_CONFIG([action-if-can-compile],
#                   [action-if-cant-compile])
# ------------------------------------------------
AC_DEFUN([MCA_opal_btl_smrocm_CONFIG],[
    AC_CONFIG_FILES([opal/mca/btl/smrocm/Makefile])

    OPAL_CHECK_ROCM([btl_smrocm])

    # Only build if ROCM support is available
    AS_IF([test "x$ROCM_SUPPORT" = "x1"],
          [$1
           OPAL_MCA_CHECK_DEPENDENCY([opal], [btl], [smrocm], [opal], [common], [sm])],
          [$2])

    AC_SUBST([btl_smrocm_CPPFLAGS])
    AC_SUBST([btl_smrocm_LDFLAGS])
    AC_SUBST([btl_smrocm_LIBS])
])dnl
