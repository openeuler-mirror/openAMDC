/*
 * Copyright (c) 2024, Apusic
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *        http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

/* This structure represents our canvas. Drawing functions will take a pointer
 * to a canvas to write to it. Later the canvas can be rendered to a string
 * suitable to be printed on the screen, using unicode Braille characters. */

/* This represents a very simple generic canvas in order to draw stuff.
 * It's up to each LOLWUT versions to translate what they draw to the
 * screen, depending on the result to accomplish. */

#ifndef __LOLWUT_H
#define __LOLWUT_H

typedef struct lwCanvas {
    int width;
    int height;
    char *pixels;
} lwCanvas;

/* Drawing functions implemented inside lolwut.c. */
lwCanvas *lwCreateCanvas(int width, int height, int bgcolor);
void lwFreeCanvas(lwCanvas *canvas);
void lwDrawPixel(lwCanvas *canvas, int x, int y, int color);
int lwGetPixel(lwCanvas *canvas, int x, int y);
void lwDrawLine(lwCanvas *canvas, int x1, int y1, int x2, int y2, int color);
void lwDrawSquare(lwCanvas *canvas, int x, int y, float size, float angle, int color);

#endif
