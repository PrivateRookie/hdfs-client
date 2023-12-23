use crc::Crc;

pub const CRC32: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_CKSUM);
pub const CRC32C: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISCSI);
