/*
 * Copyright (c) 2019       University of Houston. All rights reserved.
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_FS_DAOS_H
#define MCA_FS_DAOS_H

#include "ompi_config.h"
#include "ompi/mca/mca.h"
#include "ompi/mca/fs/fs.h"
#include "ompi/mca/common/ompio/common_ompio.h"

extern int mca_fs_daos_priority;

BEGIN_C_DECLS

int mca_fs_daos_component_init_query(bool enable_progress_threads,
                                        bool enable_mpi_threads);
struct mca_fs_base_module_1_0_0_t *
mca_fs_daos_component_file_query (ompio_file_t *fh, int *priority);
int mca_fs_daos_component_file_unquery (ompio_file_t *file);

int mca_fs_daos_module_init (ompio_file_t *file);
int mca_fs_daos_module_finalize (ompio_file_t *file);

OMPI_MODULE_DECLSPEC extern mca_fs_base_component_2_0_0_t mca_fs_daos_component;
/*
 * ******************************************************************
 * ********* functions which are implemented in this module *********
 * ******************************************************************
 */

int mca_fs_daos_file_open (struct ompi_communicator_t *comm,
                          const char *filename,
                          int amode,
                          struct opal_info_t *info,
                          ompio_file_t *fh);

/*
 * ******************************************************************
 * ************ functions implemented in this module end ************
 * ******************************************************************
 */

END_C_DECLS

#endif /* MCA_FS_DAOS_H */
