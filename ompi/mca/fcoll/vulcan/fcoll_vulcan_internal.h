/*
 * Copyright (c) 2024      Advanced Micro Devices, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_FCOLL_VULCAN_INTERNAL_H
#define MCA_FCOLL_VULCAN_INTERNAL_H

#include "ompi_config.h"


BEGIN_C_DECLS
/* Used for loading file-offsets per aggregator*/
typedef struct mca_io_ompio_local_io_array{
    OMPI_MPI_OFFSET_TYPE offset;
    MPI_Aint             length;
    int                  process_id;
}mca_io_ompio_local_io_array;

int mca_fcoll_vulcan_break_file_view (struct iovec *decoded_iov, int iov_count,
                                      struct iovec *local_iov_array, int local_count,
                                      struct iovec ***broken_decoded_iovs, int **broken_iov_counts,
                                      struct iovec ***broken_iov_arrays, int **broken_counts,
                                      MPI_Aint **broken_total_lengths,
                                      int stripe_count, size_t stripe_size);
int mca_fcoll_vulcan_get_configuration (ompio_file_t *fh, int num_io_procs,
                                        int num_groups, size_t max_data);
int mca_fcoll_vulcan_split_iov_array (ompio_file_t *fh, mca_common_ompio_io_array_t *work_array,
                                      int num_entries, int *last_array_pos, int *last_pos_in_field,
                                      int chunk_size );
int mca_fcoll_vulcan_minmax (ompio_file_t *fh, struct iovec *iov, int iov_count,
			     int num_aggregators, long *new_stripe_size);

static int local_heap_sort (mca_io_ompio_local_io_array *io_array,
			    int num_entries,
			    int *sorted)
{
    int i = 0;
    int j = 0;
    int left = 0;
    int right = 0;
    int largest = 0;
    int heap_size = num_entries - 1;
    int temp = 0;
    unsigned char done = 0;
    int* temp_arr = NULL;

    temp_arr = (int*)malloc(num_entries*sizeof(int));
    if (NULL == temp_arr) {
        opal_output (1, "OUT OF MEMORY\n");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    temp_arr[0] = 0;
    for (i = 1; i < num_entries; ++i) {
        temp_arr[i] = i;
    }
    /* num_entries can be a large no. so NO RECURSION */
    for (i = num_entries/2-1 ; i>=0 ; i--) {
        done = 0;
        j = i;
        largest = j;

        while (!done) {
            left = j*2+1;
            right = j*2+2;
            if ((left <= heap_size) &&
                (io_array[temp_arr[left]].offset > io_array[temp_arr[j]].offset)) {
                largest = left;
            }
            else {
                largest = j;
            }
            if ((right <= heap_size) &&
                (io_array[temp_arr[right]].offset >
                 io_array[temp_arr[largest]].offset)) {
                largest = right;
            }
            if (largest != j) {
                temp = temp_arr[largest];
                temp_arr[largest] = temp_arr[j];
                temp_arr[j] = temp;
                j = largest;
            }
            else {
                done = 1;
            }
        }
    }

    for (i = num_entries-1; i >=1; --i) {
        temp = temp_arr[0];
        temp_arr[0] = temp_arr[i];
        temp_arr[i] = temp;
        heap_size--;
        done = 0;
        j = 0;
        largest = j;

        while (!done) {
            left =  j*2+1;
            right = j*2+2;

            if ((left <= heap_size) &&
                (io_array[temp_arr[left]].offset >
                 io_array[temp_arr[j]].offset)) {
                largest = left;
            }
            else {
                largest = j;
            }
            if ((right <= heap_size) &&
                (io_array[temp_arr[right]].offset >
                 io_array[temp_arr[largest]].offset)) {
                largest = right;
            }
            if (largest != j) {
                temp = temp_arr[largest];
                temp_arr[largest] = temp_arr[j];
                temp_arr[j] = temp;
                j = largest;
            }
            else {
                done = 1;
            }
        }
        sorted[i] = temp_arr[i];
    }
    sorted[0] = temp_arr[0];

    if (NULL != temp_arr) {
        free(temp_arr);
        temp_arr = NULL;
    }
    return OMPI_SUCCESS;
}

END_C_DECLS

#endif /* MCA_FCOLL_VULCAN_INTERNAL_H */
