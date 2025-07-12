package vectorized

import (
	"runtime"
	"sync"
)

// SIMDCapabilities represents the SIMD instruction sets available on the current CPU
type SIMDCapabilities struct {
	// x86/x86_64 instruction sets
	HasSSE      bool // Streaming SIMD Extensions
	HasSSE2     bool // SSE2
	HasSSE3     bool // SSE3
	HasSSSE3    bool // Supplemental SSE3
	HasSSE41    bool // SSE4.1
	HasSSE42    bool // SSE4.2
	HasAVX      bool // Advanced Vector Extensions
	HasAVX2     bool // AVX2 (256-bit)
	HasAVX512F  bool // AVX-512 Foundation
	HasAVX512DQ bool // AVX-512 Doubleword and Quadword
	HasAVX512BW bool // AVX-512 Byte and Word
	HasFMA      bool // Fused Multiply-Add

	// ARM instruction sets
	HasNEON     bool // ARM NEON (Advanced SIMD)
	HasASIMD    bool // ARMv8 Advanced SIMD
	HasSVE      bool // Scalable Vector Extension
	HasSVE2     bool // SVE2

	// Architecture information
	Architecture string // "amd64", "arm64", etc.
	VectorWidth  int    // Maximum vector width in bits (256 for AVX2, 512 for AVX-512)
}

var (
	simdCaps     *SIMDCapabilities
	simdCapsOnce sync.Once
)

// GetSIMDCapabilities returns the SIMD capabilities of the current CPU
// This function is thread-safe and uses lazy initialization
func GetSIMDCapabilities() *SIMDCapabilities {
	simdCapsOnce.Do(func() {
		simdCaps = detectSIMDCapabilities()
	})
	return simdCaps
}

// detectSIMDCapabilities performs runtime CPU feature detection
func detectSIMDCapabilities() *SIMDCapabilities {
	caps := &SIMDCapabilities{
		Architecture: runtime.GOARCH,
	}

	switch runtime.GOARCH {
	case "amd64", "386":
		detectX86Features(caps)
	case "arm64":
		detectARMFeatures(caps)
	default:
		// Unknown architecture, no SIMD support
		caps.VectorWidth = 64 // Fallback to scalar processing
	}

	return caps
}

// detectX86Features detects x86/x86_64 SIMD features using CPUID
func detectX86Features(caps *SIMDCapabilities) {
	// Note: In a real implementation, you would use CPUID instruction
	// or the golang.org/x/sys/cpu package for accurate detection
	// For this demo, we'll use compile-time and runtime detection

	// Basic SSE support (almost universal on modern x86_64)
	if runtime.GOARCH == "amd64" {
		caps.HasSSE = true
		caps.HasSSE2 = true
		caps.HasSSE3 = true
		caps.HasSSSE3 = true
		caps.HasSSE41 = true
		caps.HasSSE42 = true
		caps.VectorWidth = 128
	}

	// For demonstration, we'll enable AVX2 detection
	// In reality, this would check CPUID leaves
	if cpuidSupportsAVX2() {
		caps.HasAVX = true
		caps.HasAVX2 = true
		caps.HasFMA = true
		caps.VectorWidth = 256
	}

	// AVX-512 detection (Intel Skylake-X and later, some AMD Zen 4)
	if cpuidSupportsAVX512() {
		caps.HasAVX512F = true
		caps.HasAVX512DQ = true
		caps.HasAVX512BW = true
		caps.VectorWidth = 512
	}
}

// detectARMFeatures detects ARM SIMD features
func detectARMFeatures(caps *SIMDCapabilities) {
	if runtime.GOARCH == "arm64" {
		// ARMv8 has mandatory Advanced SIMD (NEON) support
		caps.HasNEON = true
		caps.HasASIMD = true
		caps.VectorWidth = 128

		// SVE detection would require reading system registers
		// For demo purposes, we'll assume no SVE support
		caps.HasSVE = false
		caps.HasSVE2 = false
	}
}

// cpuidSupportsAVX2 checks if the CPU supports AVX2
func cpuidSupportsAVX2() bool {
	// In a real implementation, this would execute CPUID instruction
	// For demo, we'll return true on most modern x86_64 systems
	if runtime.GOARCH == "amd64" {
		// Assume AVX2 support for demonstration
		// Real implementation would check CPUID EAX=7, ECX=0, EBX bit 5
		return true
	}
	return false
}

// cpuidSupportsAVX512 checks if the CPU supports AVX-512
func cpuidSupportsAVX512() bool {
	// AVX-512 is less common, so we'll be more conservative
	// Real implementation would check CPUID EAX=7, ECX=0, EBX bit 16
	return false // Disabled for broader compatibility
}

// GetOptimalBatchSize returns the optimal batch size for SIMD operations
func (caps *SIMDCapabilities) GetOptimalBatchSize() int {
	// Batch size should be a multiple of the vector width
	// and large enough to amortize function call overhead
	switch caps.VectorWidth {
	case 512: // AVX-512
		return 4096 // 8 doubles per vector, 512 vectors per batch
	case 256: // AVX2
		return 2048 // 4 doubles per vector, 512 vectors per batch
	case 128: // SSE/NEON
		return 1024 // 2 doubles per vector, 512 vectors per batch
	default:
		return 512 // Scalar fallback
	}
}

// GetVectorElementCount returns how many elements of a given type fit in a vector
func (caps *SIMDCapabilities) GetVectorElementCount(dataType DataType) int {
	elementSize := getDataTypeSize(dataType)
	if elementSize == 0 {
		return 1 // Fallback for unknown types
	}
	
	vectorBytes := caps.VectorWidth / 8
	return vectorBytes / elementSize
}

// getDataTypeSize returns the size in bytes of a data type
func getDataTypeSize(dataType DataType) int {
	switch dataType {
	case INT8:
		return 1
	case INT16:
		return 2
	case INT32, FLOAT32:
		return 4
	case INT64, FLOAT64:
		return 8
	default:
		return 0 // Variable or unknown size
	}
}

// CanUseSIMD determines if SIMD should be used for a given operation
func (caps *SIMDCapabilities) CanUseSIMD(dataType DataType, elementCount int) bool {
	// Check if we have any SIMD support
	if caps.VectorWidth <= 64 {
		return false
	}

	// Check if the data type is supported
	switch dataType {
	case INT32, INT64, FLOAT32, FLOAT64:
		// These types have good SIMD support
	default:
		return false // String and other complex types need special handling
	}

	// Check if the batch is large enough to benefit from SIMD
	minElements := caps.GetVectorElementCount(dataType) * 4 // At least 4 vectors
	return elementCount >= minElements
}

// GetSIMDImplementation returns the best SIMD implementation for the current platform
func (caps *SIMDCapabilities) GetSIMDImplementation() SIMDImplementationType {
	if caps.HasAVX512F {
		return SIMD_AVX512
	}
	if caps.HasAVX2 {
		return SIMD_AVX2
	}
	if caps.HasAVX {
		return SIMD_AVX
	}
	if caps.HasSSE42 {
		return SIMD_SSE42
	}
	if caps.HasNEON || caps.HasASIMD {
		return SIMD_NEON
	}
	return SIMD_SCALAR
}

// SIMDImplementationType represents different SIMD implementations
type SIMDImplementationType int

const (
	SIMD_SCALAR SIMDImplementationType = iota // No SIMD, scalar operations
	SIMD_SSE42                               // SSE 4.2 (128-bit)
	SIMD_AVX                                 // AVX (256-bit, limited)
	SIMD_AVX2                                // AVX2 (256-bit, full integer support)
	SIMD_AVX512                              // AVX-512 (512-bit)
	SIMD_NEON                                // ARM NEON (128-bit)
	SIMD_SVE                                 // ARM SVE (variable width)
)

func (impl SIMDImplementationType) String() string {
	switch impl {
	case SIMD_SCALAR:
		return "Scalar"
	case SIMD_SSE42:
		return "SSE4.2"
	case SIMD_AVX:
		return "AVX"
	case SIMD_AVX2:
		return "AVX2"
	case SIMD_AVX512:
		return "AVX-512"
	case SIMD_NEON:
		return "NEON"
	case SIMD_SVE:
		return "SVE"
	default:
		return "Unknown"
	}
}

// PrintCapabilities prints a human-readable summary of SIMD capabilities
func (caps *SIMDCapabilities) PrintCapabilities() string {
	result := "SIMD Capabilities:\n"
	result += "  Architecture: " + caps.Architecture + "\n"
	result += "  Vector Width: " + string(rune(caps.VectorWidth)) + " bits\n"
	result += "  Implementation: " + caps.GetSIMDImplementation().String() + "\n"

	if caps.Architecture == "amd64" || caps.Architecture == "386" {
		result += "  x86 Features:\n"
		if caps.HasSSE {
			result += "    ✓ SSE\n"
		}
		if caps.HasSSE2 {
			result += "    ✓ SSE2\n"
		}
		if caps.HasSSE42 {
			result += "    ✓ SSE4.2\n"
		}
		if caps.HasAVX {
			result += "    ✓ AVX\n"
		}
		if caps.HasAVX2 {
			result += "    ✓ AVX2\n"
		}
		if caps.HasAVX512F {
			result += "    ✓ AVX-512\n"
		}
		if caps.HasFMA {
			result += "    ✓ FMA\n"
		}
	}

	if caps.Architecture == "arm64" {
		result += "  ARM Features:\n"
		if caps.HasNEON {
			result += "    ✓ NEON\n"
		}
		if caps.HasASIMD {
			result += "    ✓ Advanced SIMD\n"
		}
		if caps.HasSVE {
			result += "    ✓ SVE\n"
		}
	}

	return result
}

// SIMDInfo provides global access to SIMD capabilities
var SIMDInfo = GetSIMDCapabilities()