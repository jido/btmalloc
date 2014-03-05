#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>

#include <stdio.h>

typedef uint64_t aligned_uint;
typedef uint8_t uchar;

#define alignment (sizeof(aligned_uint))
const int rightmost = alignment - 1;
const int uchar_bits = 8;
const int uchar_mask = UINT8_MAX;

const int block_size = 512;
const int block_alignment = 512;

int fixedsize_mask[] = {
    0x1,    0x3,    0xF,    0xF };
int fixedsize_test[] = {
    0x1,    0x2,    0x4,    0xC };
int fixedsize_alignment[] = {
    1,      8,      4,      2 };
int fixedsize_block_size[] = {
    8,      504,    248,    128 };
int fixedsize_shift[] = {
    1,      2,      4,      4 };

const union
{
   uint32_t number;
   uint8_t byte[4];
} endianness_test = {1};

#define BIG_ENDIAN_CPU (endianness_test.byte[3])
#define LITTLE_ENDIAN_CPU (endianness_test.byte[0])

typedef union
{
   aligned_uint info;
   uchar byte[alignment];
} control;

#ifndef __has_builtin         // Optional of course.
  #define __has_builtin(x) 0  // Compatibility with non-clang compilers.
#endif

#if __has_builtin(__sync_bool_compare_and_swap) || defined(__GNUC__)
#define compare_and_set __sync_bool_compare_and_swap
#else
extern int compare_and_set();
#endif


/*
   Structure of an allocation block:
   
   .-------------------------.
   |                         |
   |        data     .-------|
   |                 | info  |
   '-------------------------'
   
   The data block contains either allocation memory or the size
   of an allocation.
   
   The lowest byte of the info block gives information about the
   organisation of the data:
   
   .----------------------------.-------------------------------.
   |   Lowest byte              |   Data                        |
   |----------------------------|-------------------------------|
   |     .......1               |   1-byte unaligned memory     |
   |     ......10               |   8B 8-aligned memory         |
   |     ....0100               |   4B 4-aligned memory         |
   |     ....1100               |   2B 2-aligned memory         |
   |     .....000               |   any size 8-aligned memory   |
   '------------------------------------------------------------'
   
   For fixed-size allocation memory, the rest of the info block
   comprises of a bitmap indicating if each slot is used or not.
   
   The size of the bitmap and the memory it maps is shown in the
   following table:
   
   .--------------------.-------------------.-------------------.
   |  Bytes per slot    |  Bits in bitmap   |  Memory size (B)  |
   |--------------------|-------------------|-------------------|
   |         1          |          7        |         7         |
   |         2          |         60        |       120         |
   |         4          |         60        |       240         |
   |         8          |         62        |       496         |
   '------------------------------------------------------------'
   
   For a fixed 1-byte allocation block, the bitmap and the 7
   bytes of allocation memory fit together in a 8-byte block.
   
   Other fixed size allocation blocks use 8 bytes for the bitmap.
   
   A block of 512 bytes can contain different kind of fixed-size
   allocation blocks or a single variable size allocation block.
   The last block must end on the 512 bytes boundary.
   
   The size of a variable size allocation block is 512 bytes.
   Its structure is illustrated below:
   
   .------------------------------------------------------------.
   | slot0 | slot1 |  ...                                       |
   |---------------'                                            |
   |                                                            |
   |                     .--------.----------.--------.---------|
   |                ...  | slot60 | reserved | bitmap | address |
   '------------------------------------------------------------'
   
   The info block of a variable size allocation block ends with
   the address of the block.
   
   The info block also contains a 62 bit bitmap indicating if
   each slot is free memory or not. The last slot is reserved
   for the address of the next control block (if used) or the
   wilderness area (if free).
   
   The organisation of each slot is given by its rightmost byte
   as follows:
   
   .----------------------------.-------------------------------.
   |  Rightmost byte            |  Slot contents                |
   |----------------------------|-------------------------------|
   |        .....001            |     1B 8-aligned memory       |
   |        .....010            |     2B 8-aligned memory       |
   |        .....011            |     3B 8-aligned memory       |
               .                               .
               .                               .
               .                               .
   |        .....111            |     7B 8-aligned memory       |
   |        .....000            |     memory address            |
   '------------------------------------------------------------'
   
   For sizes up to seven bytes, the memory is allocated
   directly in the slot.
   
   For sizes of eight bytes or more, the slot contains the
   address of allocation memory. It is rounded up by 8 so that
   it always ends with 000. On little endian processors, the
   address is rotated left so that the least significant byte
   always occupies the rightmost position.
   
   Areas of allocation memory are contiguous. The size of an
   area of allocation memory can be computed by taking the
   difference between two successive addresses.
   
   Areas of unused memory are tagged with the address of an
   allocation block at the boundary of each 512-byte block.

   The intent is to find the address of the allocation block
   associated with a given memory address by looking at the end
   of the 512-byte block which precedes it.
   
   .... -------------------.-----------------------------------
                 | address |            | used memory |
   .... -------------------------------------------------------
                           |
     512B block boundary --'
   
   This means that the last 8 bytes of each 512-bytes block
   must be left unallocated, unless it is part of a larger
   allocated memory area. Memory cannot be allocated between
   the end of an area of used memory larger than 504 bytes and
   the allocation block address which follows.
   
   For this reason, the end address for a memory allocation
   larger than 504 bytes falls 8 bytes before a 512-bytes block
   boundary to avoid memory wastage, unless a different align-
   ment is specified.
*/
/*
   Allocation of memory
   
   The first allocation block is at a known address. By looking
   at the lowest byte of the final control block of an alloca-
   tion block, the allocator can tell what kind of memory it
   contains.
   
   If the block contains fixed size allocation memory, then the
   next allocation block follows immediately. If the block
   contains variable size allocation memory, the reserved slot
   contains the address of the next allocation block (if there
   is one).
   
   The bitmap of a control block indicates which slots are free
   and which are used.
   
   The allocator must visit all allocation blocks until it
   finds a suitable free slot.
   
   Each thread caches the address of the most recent allocation
   blocks it used. These are checked first, starting with the
   most recent. This helps with memory locality.
   
   For sizes up to 8 bytes, a slot in a fixed allocation block
   is preferred.
   
   When a suitable free slot is found, it is marked as used in
   the bitmap and its address is returned. In case of variable
   size allocation, if the requested size is less than the
   available size a neighbouring free allocation slot is resized
   accordingly if there is one.
   
   When an allocation block gets full, a new allocation block may
   be created in the free space that follows to manage memory
   allocation inside it.
   
   Fixed size allocation blocks can be created in the free space
   between the last area of used memory and the next allocation
   block.
   
   The last allocation block in memory is always a variable size
   allocation block which manages the wilderness area.
*/
/*
   Freeing of memory
   
   The algorithm of freeing requires to find the allocation
   block the memory is attached to.
   
   The value of the last 8 bytes of the 512-byte block situated
   before the memory address is checked. If its lowest byte is 0,
   then the last 8 bytes of the 512-byte block contain the
   address of the allocation block. If it doesn't then the
   memory address is within its own allocation block.
   
   Once the allocation block is identified the bit corresponding
   to the allocated memory in the bitmap is set to zero to mark
   it as available.
*/
/*
   Concurrency and synchronisation
   
   Synchronisation relies on wait-free locking to avoid
   inconsistency due to concurrent updates.
   
   When allocating memory, the bitmap is updated first using
   compare-and-set. This ensures only one thread updates the
   bitmap at once. If the compare fails, the allocator looks
   for another block with suitable free memory. It is better
   for memory locality if different threads allocate in
   different blocks.
   
   Once the bitmap is updated the slot is initialised if
   necessary then the address of the memory is returned.
   Assuming the program using the allocator is correct no
   other thread can touch the slot after it is marked as
   allocated and before the address is returned.
   
   The slot is always read again to check that it still
   contains the expected value before being used. If the slot
   contents changed, the allocator looks again for a suitable
   free slot in the same block.
   
   When freeing memory, if the slot needs to be modified
   this is done first while the slot is marked as used, then
   the bitmap is updated using compare-and-set. If the bitmap
   compare fails the operation is restarted from the
   beginning.
   
   No operation modifies a slot which is marked as free.
   Operations involving several slots first marks all these
   slots as used with compare-and-set before continuing.
   
   To resize an area of free memory, the free slot is first
   marked as used then the address pointed by the next slot
   is adjusted. No other thread must be allowed to modify the
   address pointed by the next slot.
   
   If the resize happens during allocation, then the next slot
   gets marked as used at the same time as the area of free
   memory to resize. If the resize happens during creation of
   a new allocation block, then an imaginary 63rd slot is
   marked as used at the same time as the area of free memory
   to resize. No other thread can create a new allocation
   block if this imaginary slot is used.
*/


/*
   Bit fiddling functions for the address of an allocation
   area
*/

// Quickly rotate the value least significant byte (LSB) so
// that the destination ends with 000
void rotate(aligned_uint value, control *const destination)
{
    assert( value / alignment * alignment == value );
    
    if ( BIG_ENDIAN_CPU || sizeof (void*) < alignment )
    {
        // The LSB is already in rightmost position if the CPU
        // is big endian. If the CPU is little endian but
        // addresses are less than 64 bits, the most signifi-
        // cant byte (MSB) is always 0 so no need to rotate.
        destination->info = value;
        
        assert( BIG_ENDIAN_CPU || destination->byte[rightmost] == 0 );
    }
    else
    {
        assert( LITTLE_ENDIAN_CPU );
        
        destination->info = value >> uchar_bits;
        destination->byte[rightmost] = (uchar) value;
    }
}

// Rotate in reverse direction to move the LSB back to its
// place so the address can be used
aligned_uint unrotate(const control *const value)
{
    if ( BIG_ENDIAN_CPU || sizeof (void*) < alignment )
    {
        // Value is not rotated
        return value->info;
    }
    assert( LITTLE_ENDIAN_CPU );
        
    // Rotate backwards
    return (value->info << uchar_bits) | value->byte[rightmost];
}

// Find the allocation block which manages the specified address
aligned_uint *allocation_block(const void *const allocated)
{
    // Check the info block which precedes the 512-bytes block boundary
    aligned_uint *boundary = (aligned_uint*) ((uintptr_t) allocated & ~(block_size - 1));
    aligned_uint info = *(boundary - 1);
    
    if ( info & uchar_mask )
    {
        // The memory is allocated within this 512-bytes block
        return boundary;
    }
    else
    {
        // The info block indicates the address of the allocation block
        assert( info < (uintptr_t) boundary );
        return (aligned_uint*) info;
    }
}

void free_fixed_size_memory(const void *const allocated, aligned_uint *const block)
{
    // Address of bitmap
    aligned_uint *bitmap = block + (block_size / alignment - 1);
    aligned_uint *next = NULL;
    aligned_uint b;
    
    // Look for the right block within the allocation block
    do {
        b = *bitmap;
        assert( b != 0 );
        assert( ( ( b & 1 ) && ( (uintptr_t) allocated / alignment == (uintptr_t) bitmap / alignment ) ) || allocated < (void*) bitmap );
        
        // Identify the slot size
        int slot_type;
        for ( slot_type = 0; slot_type < sizeof fixedsize_mask; ++slot_type )
        {
            if ( (b & fixedsize_mask[slot_type]) == fixedsize_test[slot_type] )
            {
                // Save the location of the next bitmap (or block info)
                next = bitmap - (fixedsize_block_size[slot_type] / alignment);
                break;
            }
        }
        assert( next != NULL );
        
        // Check if memory belongs to this block
        if ( next != NULL && allocated >= (void*) (next + 1) )
        {
            // Found the right block
            intptr_t offset = (allocated - (void*) (next + 1)) / fixedsize_alignment[slot_type];
            do {
                // Free memory (busy loop)
                b = *bitmap;
                aligned_uint freed = b & ~(1 << (fixedsize_shift[slot_type] + offset));
                assert( freed != b );   // No other thread should free the slot
                if ( compare_and_set(bitmap, freed, b) )
                {
                    // Success!
                    return;
                }
            } while (1);
        }
        // Continue to next block
        bitmap = next;
    } while ( bitmap != NULL );
}


int main(int n, char* args[])
{
    aligned_uint a = 0x123456789ABCDEF0L;
    control b = {0xDABADABADABADABAL};
    printf("a=%llx, b=%llx\n", a, b.info);
    rotate(a, &b);
    printf("b'=%llx, a'=%llx\n", b.info, unrotate(&b));
    return 0;
}
