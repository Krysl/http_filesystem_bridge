use winapi::um::winnt;

pub fn access_flags_to_string(desired_access: winnt::ACCESS_MASK) -> String {
    // 将访问权限转换为字符串
    // 这里使用了一个简单的字符串拼接方法来表示访问权限
    // 实际应用中可能需要更复杂的处理
    if desired_access == 0 {
        return "NO_ACCESS".to_string();
    } else {
        // 创建一个空的向量来存储访问权限标志
        let mut access_flags = Vec::new();
        // 检查并记录请求的访问权限
        if desired_access & winnt::GENERIC_READ > 0 {
            access_flags.push("GENERIC_READ");
        }
        if desired_access & winnt::GENERIC_WRITE > 0 {
            access_flags.push("GENERIC_WRITE");
        }
        if desired_access & winnt::GENERIC_EXECUTE > 0 {
            access_flags.push("GENERIC_EXECUTE");
        }
        if desired_access & winnt::DELETE > 0 {
            access_flags.push("DELETE");
        }
        if desired_access & winnt::READ_CONTROL > 0 {
            access_flags.push("READ_CONTROL");
        }
        if desired_access & winnt::WRITE_DAC > 0 {
            access_flags.push("WRITE_DAC");
        }
        if desired_access & winnt::WRITE_OWNER > 0 {
            access_flags.push("WRITE_OWNER");
        }
        if desired_access & winnt::SYNCHRONIZE > 0 {
            access_flags.push("SYNCHRONIZE");
        }
        if desired_access & winnt::FILE_READ_DATA > 0 {
            access_flags.push("FILE_READ_DATA");
        }
        if desired_access & winnt::FILE_WRITE_DATA > 0 {
            access_flags.push("FILE_WRITE_DATA");
        }
        if desired_access & winnt::FILE_APPEND_DATA > 0 {
            access_flags.push("FILE_APPEND_DATA");
        }
        if desired_access & winnt::FILE_READ_EA > 0 {
            access_flags.push("FILE_READ_EA");
        }
        if desired_access & winnt::FILE_WRITE_EA > 0 {
            access_flags.push("FILE_WRITE_EA");
        }
        if desired_access & winnt::FILE_EXECUTE > 0 {
            access_flags.push("FILE_EXECUTE");
        }
        if desired_access & winnt::FILE_DELETE_CHILD > 0 {
            access_flags.push("FILE_DELETE_CHILD");
        }
        if desired_access & winnt::FILE_READ_ATTRIBUTES > 0 {
            access_flags.push("FILE_READ_ATTRIBUTES");
        }
        if desired_access & winnt::FILE_WRITE_ATTRIBUTES > 0 {
            access_flags.push("FILE_WRITE_ATTRIBUTES");
        }
        access_flags.join(" | ") // 将权限标志拼接为字符串
    }
}
