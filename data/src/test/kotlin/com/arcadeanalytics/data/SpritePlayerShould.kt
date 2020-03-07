/*-
 * #%L
 * Arcade Data
 * %%
 * Copyright (C) 2018 - 2019 ArcadeAnalytics
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.arcadeanalytics.data

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

class SpritePlayerShould {

    @Test
    internal fun `mark the sprite with a new field `() {

        val player: SpritePlayer = object : SpritePlayer {
            override fun end() {
            }

            override fun begin() {
            }

            override fun play(sprite: Sprite) {
                sprite.add("mark", "marked")
            }
        }

        val sprite = Sprite()
        player.play(sprite)

        Assertions.assertThat(sprite.hasField("mark")).isTrue()
        Assertions.assertThat(sprite.valueOf("mark")).isEqualTo("marked")

    }

    @Test
    internal fun `accept empty sprite`() {

        val player: SpritePlayer = object : SpritePlayer {
            override fun begin() {
            }

            override fun end() {
            }

            override fun accept(sprite: Sprite): Boolean {
                return sprite.isEmpty()
            }

            override fun play(sprite: Sprite) {
                sprite.add("mark", "marked")
            }
        }

        val sprite = Sprite()

        Assertions.assertThat(player.accept(sprite)).isTrue()

    }


}
