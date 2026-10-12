#include "raft_storage.h"

#include <cstdio>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>
#include <system_error>

#ifdef _WIN32

#include <windows.h>

#else

#include <fcntl.h>
#include <unistd.h>

#endif

using namespace std;

namespace {

// ============================================================
// Hex Encoding
//
// Commands can contain spaces:
//
// PUT 123 user:1 Alice Smith
//
// Hex encoding makes each WAL entry easy to parse.
// ============================================================

string hexEncode(
    const string& input
) {
    static const char*
        digits =
            "0123456789ABCDEF";

    string output;

    output.reserve(
        input.size() * 2
    );

    for (
        unsigned char c :
        input
    ) {
        output.push_back(
            digits[
                c >> 4
            ]
        );

        output.push_back(
            digits[
                c & 0x0F
            ]
        );
    }

    return output;
}

int hexValue(
    char c
) {
    if (
        c >= '0' &&
        c <= '9'
    ) {
        return
            c - '0';
    }

    if (
        c >= 'a' &&
        c <= 'f'
    ) {
        return
            c - 'a' + 10;
    }

    if (
        c >= 'A' &&
        c <= 'F'
    ) {
        return
            c - 'A' + 10;
    }

    return -1;
}

bool hexDecode(
    const string& input,
    string& output
) {
    if (
        input.size() % 2 != 0
    ) {
        return false;
    }

    output.clear();

    output.reserve(
        input.size() / 2
    );

    for (
        size_t i = 0;
        i < input.size();
        i += 2
    ) {
        int high =
            hexValue(
                input[i]
            );

        int low =
            hexValue(
                input[i + 1]
            );

        if (
            high < 0 ||
            low < 0
        ) {
            return false;
        }

        output.push_back(
            static_cast<char>(
                (
                    high << 4
                ) |
                low
            )
        );
    }

    return true;
}

// ============================================================
// Force File To Disk
// ============================================================

bool syncFile(
    const filesystem::path& path,
    string& error
) {
#ifdef _WIN32

    HANDLE handle =
        CreateFileA(
            path.string().c_str(),
            GENERIC_READ |
                GENERIC_WRITE,
            FILE_SHARE_READ |
                FILE_SHARE_WRITE,
            nullptr,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            nullptr
        );

    if (
        handle ==
        INVALID_HANDLE_VALUE
    ) {
        error =
            "failed to open file for FlushFileBuffers: " +
            path.string();

        return false;
    }

    BOOL result =
        FlushFileBuffers(
            handle
        );

    CloseHandle(
        handle
    );

    if (!result) {
        error =
            "FlushFileBuffers failed: " +
            path.string();

        return false;
    }

#else

    int fd =
        open(
            path.c_str(),
            O_RDWR
        );

    if (
        fd < 0
    ) {
        error =
            "failed to open file for fsync: " +
            path.string();

        return false;
    }

    int result =
        fsync(
            fd
        );

    close(
        fd
    );

    if (
        result != 0
    ) {
        error =
            "fsync failed: " +
            path.string();

        return false;
    }

#endif

    return true;
}

// ============================================================
// Replace Metadata File
// ============================================================

bool replaceFile(
    const filesystem::path& temporary,
    const filesystem::path& target,
    string& error
) {
#ifdef _WIN32

    BOOL result =
        MoveFileExA(
            temporary.string().c_str(),
            target.string().c_str(),
            MOVEFILE_REPLACE_EXISTING |
                MOVEFILE_WRITE_THROUGH
        );

    if (!result) {
        error =
            "failed to replace file: " +
            target.string();

        return false;
    }

#else

    if (
        std::rename(
            temporary.c_str(),
            target.c_str()
        ) != 0
    ) {
        error =
            "failed to replace file: " +
            target.string();

        return false;
    }

#endif

    return true;
}

} // namespace

// ============================================================
// Constructor
// ============================================================

RaftStorage::RaftStorage(
    const filesystem::path& directory
)
    : directoryPath(
          directory
      ),

      metadataPath(
          directory /
          "metadata.state"
      ),

      walPath(
          directory /
          "raft.log"
      ) {
}

// ============================================================
// Initialize
// ============================================================

bool RaftStorage::initialize(
    string& error
) {
    error_code ec;

    filesystem::create_directories(
        directoryPath,
        ec
    );

    if (ec) {
        error =
            "failed to create Raft directory: " +
            directoryPath.string();

        return false;
    }

    // --------------------------------------------------------
    // Create metadata file if missing.
    // --------------------------------------------------------

    if (
        !filesystem::exists(
            metadataPath
        )
    ) {
        if (
            !persistMetadata(
                0,
                "",
                0,
                error
            )
        ) {
            return false;
        }
    }

    // --------------------------------------------------------
    // Create empty WAL if missing.
    // --------------------------------------------------------

    if (
        !filesystem::exists(
            walPath
        )
    ) {
        ofstream output(
            walPath,
            ios::out |
                ios::trunc
        );

        if (!output) {
            error =
                "failed to create Raft WAL";

            return false;
        }

        output.close();

        if (
            !syncFile(
                walPath,
                error
            )
        ) {
            return false;
        }
    }

    return true;
}

// ============================================================
// Load Persistent State
// ============================================================

bool RaftStorage::load(
    uint64_t& currentTerm,
    string& votedFor,
    uint64_t& commitIndex,
    vector<RaftLogEntry>& log,
    string& error
) {
    // ========================================================
    // Metadata
    // ========================================================

    ifstream metadata(
        metadataPath
    );

    if (!metadata) {
        error =
            "failed to open Raft metadata";

        return false;
    }

    string magic;

    getline(
        metadata,
        magic
    );

    if (
        magic !=
        "HYDRA_RAFT_METADATA_V1"
    ) {
        error =
            "invalid Raft metadata header";

        return false;
    }

    string key;

    if (
        !(metadata
          >> key
          >> currentTerm) ||
        key !=
            "term"
    ) {
        error =
            "invalid Raft term metadata";

        return false;
    }

    string vote;

    if (
        !(metadata
          >> key
          >> vote) ||
        key !=
            "vote"
    ) {
        error =
            "invalid Raft vote metadata";

        return false;
    }

    votedFor =
        vote == "-"
            ? ""
            : vote;

    if (
        !(metadata
          >> key
          >> commitIndex) ||
        key !=
            "commit"
    ) {
        error =
            "invalid Raft commit metadata";

        return false;
    }

    // ========================================================
    // WAL
    // ========================================================

    ifstream wal(
        walPath
    );

    if (!wal) {
        error =
            "failed to open Raft WAL";

        return false;
    }

    log.clear();

    // Sentinel.
    log.push_back(
        {
            0,
            0,
            ""
        }
    );

    string line;

    while (
        getline(
            wal,
            line
        )
    ) {
        if (
            line.empty()
        ) {
            continue;
        }

        istringstream input(
            line
        );

        uint64_t index;

        uint64_t term;

        string encodedCommand;

        if (
            !(input
              >> index
              >> term
              >> encodedCommand)
        ) {
            error =
                "malformed Raft WAL entry";

            return false;
        }

        if (
            index !=
            log.size()
        ) {
            error =
                "non-sequential Raft WAL index";

            return false;
        }

        string command;

        if (
            !hexDecode(
                encodedCommand,
                command
            )
        ) {
            error =
                "invalid encoded Raft command";

            return false;
        }

        log.push_back(
            {
                index,
                term,
                command
            }
        );
    }

    if (
        commitIndex >
        log.size() - 1
    ) {
        error =
            "commit index exceeds Raft log";

        return false;
    }

    return true;
}

// ============================================================
// Persist Metadata
// ============================================================

bool RaftStorage::persistMetadata(
    uint64_t currentTerm,
    const string& votedFor,
    uint64_t commitIndex,
    string& error
) {
    filesystem::path temporary =
        metadataPath;

    temporary +=
        ".tmp";

    {
        ofstream output(
            temporary,
            ios::out |
                ios::trunc
        );

        if (!output) {
            error =
                "failed to create temporary Raft metadata";

            return false;
        }

        output
            << "HYDRA_RAFT_METADATA_V1\n";

        output
            << "term "
            << currentTerm
            << '\n';

        output
            << "vote "
            << (
                votedFor.empty()
                    ? "-"
                    : votedFor
            )
            << '\n';

        output
            << "commit "
            << commitIndex
            << '\n';

        output.flush();

        if (!output) {
            error =
                "failed writing Raft metadata";

            return false;
        }
    }

    if (
        !syncFile(
            temporary,
            error
        )
    ) {
        return false;
    }

    return
        replaceFile(
            temporary,
            metadataPath,
            error
        );
}

// ============================================================
// Append WAL Entry
// ============================================================

bool RaftStorage::appendEntry(
    const RaftLogEntry& entry,
    string& error
) {
    {
        ofstream output(
            walPath,
            ios::out |
                ios::app
        );

        if (!output) {
            error =
                "failed to open Raft WAL for append";

            return false;
        }

        output
            << entry.index
            << ' '
            << entry.term
            << ' '
            << hexEncode(
                   entry.command
               )
            << '\n';

        output.flush();

        if (!output) {
            error =
                "failed writing Raft WAL";

            return false;
        }
    }

    return
        syncFile(
            walPath,
            error
        );
}

// ============================================================
// Rewrite WAL
//
// Required when a follower has a conflicting suffix.
// ============================================================

bool RaftStorage::rewriteLog(
    const vector<RaftLogEntry>& log,
    string& error
) {
    filesystem::path temporary =
        walPath;

    temporary +=
        ".tmp";

    {
        ofstream output(
            temporary,
            ios::out |
                ios::trunc
        );

        if (!output) {
            error =
                "failed to create temporary Raft WAL";

            return false;
        }

        // Skip sentinel index 0.
        for (
            size_t i = 1;
            i < log.size();
            ++i
        ) {
            output
                << log[i].index
                << ' '
                << log[i].term
                << ' '
                << hexEncode(
                       log[i].command
                   )
                << '\n';
        }

        output.flush();

        if (!output) {
            error =
                "failed writing rewritten Raft WAL";

            return false;
        }
    }

    if (
        !syncFile(
            temporary,
            error
        )
    ) {
        return false;
    }

    return
        replaceFile(
            temporary,
            walPath,
            error
        );
}

// ============================================================
// Directory
// ============================================================

string
RaftStorage::getDirectoryPath() const {
    return
        directoryPath.string();
}