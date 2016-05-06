// g++ -o t -lncurses

#include <ncurses.h>
#include <unistd.h>
#include <cstdlib>

int main()
{
	char users_name[ 100 ];

	initscr();
    start_color();
    nodelay(stdscr, TRUE);

    init_pair(1, COLOR_WHITE, COLOR_BLACK);
    init_pair(2, COLOR_RED  , COLOR_BLACK);
    init_pair(3, COLOR_GREEN, COLOR_BLACK);

	int n;
	double px = 250.00;

	for (int i=0; i < 100; i++) {
		n   = rand() % 15;
		n  += (n - n % 5);
		n  *= rand() % 3 - 1;
		px += (double)n / 100;

		clear();
        attron(COLOR_PAIR(1));
        printw("KRX Futures:\n");
        attron(COLOR_PAIR(2));
		printw("  %10c %9.2f %10d\n", ' ', px + 0.10, rand() % 30);
		printw("  %10c %9.2f %10d\n", ' ', px + 0.05, rand() % 30);
		printw("  %10c %9.2f %10d\n", ' ', px,        rand() % 30);
        attron(COLOR_PAIR(3));
		printw("  %10d %9.2f %10c\n", rand() % 30, px - 0.05, ' ');
		printw("  %10d %9.2f %10c\n", rand() % 30, px - 0.10, ' ');
		printw("  %10d %9.2f %10c\n", rand() % 30, px - 0.15, ' ');

		/* Here is where we clear the screen.                  */
		/* (Remember, when using Curses, no change will appear */
		/* on the screen until <b>refresh</b>() is called.     */

		refresh();

        int ch = getch();
        if (ch != ERR) {
            ungetch(ch);
            break;
        }

        usleep(500000);
	}
    endwin();
	return 0;
}
