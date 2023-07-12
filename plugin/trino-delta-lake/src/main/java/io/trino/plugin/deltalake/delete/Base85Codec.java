/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.deltalake.delete;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.LongMath;
import com.google.common.primitives.SignedBytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

// This implements Base85 using the 4 byte block aligned encoding and character set from Z85 https://rfc.zeromq.org/spec/32
// Delta Lake implementation is https://github.com/delta-io/delta/blob/master/kernel/kernel-api/src/main/java/io/delta/kernel/internal/deletionvectors/Base85Codec.java
public final class Base85Codec
{
    @VisibleForTesting
    static final long BASE = 85L;
    @VisibleForTesting
    static final long BASE_2ND_POWER = LongMath.pow(BASE, 2);
    @VisibleForTesting
    static final long BASE_3RD_POWER = LongMath.pow(BASE, 3);
    @VisibleForTesting
    static final long BASE_4TH_POWER = LongMath.pow(BASE, 4);

    private static final int ASCII_BITMASK = 0x7F;

    // UUIDs always encode into 20 characters
    static final int ENCODED_UUID_LENGTH = 20;

    private static final String BASE85_CHARACTERS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

    @VisibleForTesting
    static final byte[] ENCODE_MAP = BASE85_CHARACTERS.getBytes(UTF_8);

    // The bitmask is the same as largest possible value, so the length of the array must be one greater.
    static final byte[] DECODE_MAP = new byte[ASCII_BITMASK + 1];

    static {
        // Following loop doesn't fill all values
        Arrays.fill(DECODE_MAP, (byte) -1);
        for (int i = 0; i < ENCODE_MAP.length; i++) {
            DECODE_MAP[ENCODE_MAP[i]] = SignedBytes.checkedCast(i);
        }
    }

    private Base85Codec() {}

    public static ByteBuffer decodeBlocks(String encoded)
    {
        char[] input = encoded.toCharArray();
        checkArgument(input.length % 5 == 0, "Input should be 5 character aligned");
        ByteBuffer buffer = ByteBuffer.allocate(input.length / 5 * 4);

        // A mechanism to detect invalid characters in the input while decoding, that only has a
        // single conditional at the very end, instead of branching for every character.
        class InputCharDecoder
        {
            int canary;

            long decodeInputChar(int i)
            {
                char c = input[i];
                canary |= c; // non-ascii char has bits outside of ASCII_BITMASK
                byte b = DECODE_MAP[c & ASCII_BITMASK];
                canary |= b; // invalid char maps to -1, which has bits outside ASCII_BITMASK
                return b;
            }
        }

        int inputIndex = 0;
        InputCharDecoder inputCharDecoder = new InputCharDecoder();
        while (buffer.hasRemaining()) {
            int sum = 0;
            sum += inputCharDecoder.decodeInputChar(inputIndex) * BASE_4TH_POWER;
            sum += inputCharDecoder.decodeInputChar(inputIndex + 1) * BASE_3RD_POWER;
            sum += inputCharDecoder.decodeInputChar(inputIndex + 2) * BASE_2ND_POWER;
            sum += inputCharDecoder.decodeInputChar(inputIndex + 3) * BASE;
            sum += inputCharDecoder.decodeInputChar(inputIndex + 4);
            buffer.putInt(sum);
            inputIndex += 5;
        }
        checkArgument((inputCharDecoder.canary & ~ASCII_BITMASK) == 0, "Input is not valid Z85: %s", encoded);
        buffer.rewind();
        return buffer;
    }
}
