"""
ENHANCED TRADING - STRATEGY AAA FAITHFUL IMPLEMENTATION
Author: Trading System
Version: 5.0-STRATEGY-AAA-FAITHFUL
Description: Strategy AAA - Faithful flowchart implementation
"""

import asyncio
import csv
import json
import logging
import os
import random
import signal
import sys
import threading
import time
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from art import text2art
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException
from rich.align import Align
from rich.box import DOUBLE, HEAVY, ROUNDED, SIMPLE
from rich.columns import Columns
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.rule import Rule
from rich.style import Style
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

# ============================================================================
# ENHANCED VISUAL CONFIGURATION
# ============================================================================

# Enhanced font mapping with more artistic options
FONT_MAPPING = {
    "default": "standard",
    "triceratops": "slant",
    "trex": "3d",
    "happy": "block",
    "raptor": "small",
    "sleep": "mini",
    "trex_roar": "doom",
    "sad": "straight",
    "dead": "sub-zero",
    "victory": "starwars",
    "alert": "univers",
    "profit": "epic",
    "loss": "poison",
    "buy": "larry3d",
    "sell": "ogre",
    "signal": "isometric1",
    "mode": "cyberlarge",
    "strategy": "roman",
    "status": "big",
    "startup": "chunky",
    "shutdown": "graffiti",
    "header": "colossal",
    "subheader": "big",
}

# Trading configuration
SYMBOL = "SOLFDUSD"
TOKEN = "SOL"
PAIR = "FDUSD"
TRADE_AMOUNT_USD = 5.2
MIN_TOKEN_VALUE_FOR_SELL = 5.2
DAILY_DIFF_LOWER_LIMIT = -1.0
DAILY_DIFF_UPPER_LIMIT = 4.0

# Files
INDICATORS_CSV = "pinescript_indicators.csv"
TRANSACTIONS_CSV = "transactions.csv"
STATE_FILE = "trading_state.json"
ERROR_LOG_CSV = "trading_errors.csv"

# Safety settings
ORDER_COOLDOWN_SECONDS = 1
MAX_ORDER_RETRIES = 3
ORDER_TIMEOUT_SECONDS = 30
TEST_MODE = False

# CSV monitoring
CSV_UPDATE_CHECK_INTERVAL = 2
CSV_STALE_THRESHOLD = 30
CSV_BATCH_PROCESSING = True

# Display settings - ENHANCED
STATUS_UPDATE_INTERVAL = 10
VERBOSE_LOGGING = True
ANIMATION_SPEED = 0.03
PULSE_EFFECT = True
GRADIENT_COLORS = True
ENHANCED_BORDERS = True

# ============================================================================
# ENHANCED COLOR MANAGEMENT
# ============================================================================


class Colors:
    """Enhanced color definitions with gradients and effects"""

    # Base colors
    SUCCESS = "bright_green"
    ERROR = "bright_red"
    WARNING = "bright_yellow"
    INFO = "bright_cyan"
    MAGENTA = "bright_magenta"
    WHITE = "bright_white"
    BLUE = "bright_blue"
    GRAY = "grey70"

    # Enhanced gradient colors
    PROFIT = "green_yellow"
    LOSS = "red1"
    NEUTRAL = "yellow3"
    HIGHLIGHT = "cyan2"
    ACCENT = "magenta2"
    DIM = "grey50"
    GOLD = "gold3"
    SILVER = "grey85"
    BRONZE = "dark_orange3"

    # Status colors
    ACTIVE = "spring_green2"
    IDLE = "deep_sky_blue3"
    WAITING = "orange3"
    PROCESSING = "dark_violet"
    COMPLETE = "green4"

    # Strategy colors - Only AAA now
    STRATEGY_AAA = "chartreuse3"

    # Phase colors
    ENTRY = "dodger_blue2"
    MONITORING = "medium_purple3"
    POSITION = "green3"
    EXIT = "orange_red1"
    STOP_LOSS = "red3"

    @staticmethod
    def get_strategy_color(variant):
        """Get enhanced strategy color - Always AAA now"""
        return Colors.STRATEGY_AAA

    @staticmethod
    def get_mode_color(mode):
        """Get enhanced mode color"""
        if mode == "BUY":
            return "bright_green"
        elif mode == "SELL":
            return "bright_red"
        else:
            return "bright_yellow"

    @staticmethod
    def get_phase_color(phase):
        """Get enhanced phase color"""
        phase_map = {
            "ENTRY_MONITORING": Colors.ENTRY,
            "ENTRY_SIGNAL_CONFIRMED": Colors.PROCESSING,
            "POSITION_OPEN": Colors.POSITION,
            "EXIT_MONITORING": Colors.EXIT,
            "STOP_LOSS_ACTIVE": Colors.STOP_LOSS,
        }
        return phase_map.get(phase, Colors.INFO)

    @staticmethod
    def get_gradient_text(text, color1, color2):
        """Create gradient text effect"""
        result = Text()
        length = len(text)
        for i, char in enumerate(text):
            ratio = i / max(length - 1, 1)
            if ratio < 0.5:
                result.append(char, style=f"{color1}")
            else:
                result.append(char, style=f"{color2}")
        return result

    @staticmethod
    def get_pulse_effect(text, base_color):
        """Create pulsing text effect"""
        if not PULSE_EFFECT:
            return Text(text, style=base_color)

        pulse_phase = (time.time() * 2) % (2 * 3.14159)
        intensity = abs(int((np.sin(pulse_phase) + 1) * 5))
        return Text(text, style=f"{base_color} bold" if intensity > 5 else base_color)

    @staticmethod
    def get_random_emoji(category="general"):
        """Get random emoji for visual variety"""
        emoji_sets = {
            "general": ["✨", "🌟", "⚡", "💫", "🔥", "🎯", "🚀", "💎", "🎰", "🎲", "🎪", "🎨"],
            "success": ["✅", "🎉", "🏆", "🥇", "💰", "💸", "💵", "💴", "💶", "💷", "💳"],
            "error": ["❌", "💥", "💣", "☠️", "⚰️", "👻", "👽", "🤖", "👾", "💀"],
            "warning": ["⚠️", "🔔", "📢", "🚨", "👀", "🔍", "🔎", "📡", "📟", "📻"],
            "trading": ["📈", "📉", "💹", "🔺", "🔻", "↗️", "↘️", "⬆️", "⬇️", "↔️"],
            "animals": ["🦕", "🦖", "🐉", "🐲", "🦎", "🦂", "🕷️", "🦇", "🦅", "🦉"],
        }

        return random.choice(emoji_sets.get(category, emoji_sets["general"]))


# ============================================================================
# ENHANCED ART AND ANIMATION FUNCTIONS
# ============================================================================


class VisualEffects:
    """Enhanced visual effects and animations"""

    @staticmethod
    def print_with_animation(
        text, color=Colors.INFO, style="", emoji="", indent=0, speed=ANIMATION_SPEED
    ):
        """Print text with typing animation effect"""
        indent_str = " " * indent
        full_text = f"{indent_str}{emoji} {text}"

        console = Console()
        styled_text = Text()

        if style == "bold":
            style_tag = "bold "
        elif style == "italic":
            style_tag = "italic "
        elif style == "underline":
            style_tag = "underline "
        elif style == "blink":
            style_tag = "blink "
        else:
            style_tag = ""

        for i, char in enumerate(full_text):
            styled_text.append(char, style=f"{style_tag}{color}")
            console.print(styled_text, end="\r")
            time.sleep(speed)
        console.print()

    @staticmethod
    def display_animated_dino(
        message, dino_type="default", title="", border_style="double"
    ):
        """Display animated ASCII art"""
        try:
            font = FONT_MAPPING.get(dino_type, "standard")
            dino = text2art(message, font=font)

            if title:
                title_text = Colors.get_gradient_text(
                    f"  {title}  ", Colors.GOLD, Colors.ACCENT
                )
                console = Console()

                if ENHANCED_BORDERS:
                    border_char = {"double": "═", "heavy": "█", "rounded": "─"}.get(
                        border_style, "─"
                    )
                    border_length = max(len(line) for line in dino.split("\n"))
                    top_border = border_char * (border_length + 4)
                    bottom_border = border_char * (border_length + 4)

                    console.print(Text(top_border, style=Colors.GOLD))
                    console.print(Align.center(title_text))
                    console.print(Text(dino, style=Colors.HIGHLIGHT))
                    console.print(Text(bottom_border, style=Colors.GOLD))
                else:
                    console.print(Text(dino, style=Colors.HIGHLIGHT))

        except Exception as e:
            console = Console()
            fallback_text = f"\n🦕 {message}\n"
            console.print(Text(fallback_text, style=Colors.INFO))

    @staticmethod
    def create_progress_bar(percentage, width=30, label=""):
        """Create enhanced visual progress bar"""
        filled = int(width * percentage / 100)
        empty = width - filled

        if percentage >= 80:
            bar_char = "█"
            color = Colors.PROFIT
        elif percentage >= 50:
            bar_char = "▓"
            color = Colors.WARNING
        else:
            bar_char = "▒"
            color = Colors.INFO

        bar = f"[{color}]{bar_char * filled}[/{color}][{Colors.DIM}]{'░' * empty}[/{Colors.DIM}]"

        if percentage >= 90:
            perc_color = Colors.PROFIT
        elif percentage >= 70:
            perc_color = Colors.SUCCESS
        elif percentage >= 50:
            perc_color = Colors.WARNING
        elif percentage >= 30:
            perc_color = Colors.INFO
        else:
            perc_color = Colors.ERROR

        perc_text = f"[{perc_color}]{percentage:.1f}%[/{perc_color}]"

        return f"{label} {bar} {perc_text}"

    @staticmethod
    def display_spinner(message, color=Colors.INFO):
        """Display animated spinner with message"""
        spinner_chars = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        console = Console()

        for i in range(10):
            spinner = spinner_chars[i % len(spinner_chars)]
            text = Text(f"{spinner} {message}", style=color)
            console.print(text, end="\r")
            time.sleep(0.1)
        console.print(Text(f"✓ {message}", style=Colors.SUCCESS))

    @staticmethod
    def flash_border(color=Colors.WARNING, length=80, duration=0.5):
        """Flash border effect"""
        console = Console()
        for _ in range(3):
            console.print(Text("█" * length, style=color))
            time.sleep(duration)
            console.print("\033[F\033[K", end="")
            time.sleep(duration / 2)


# ============================================================================
# ENHANCED PRINT FUNCTIONS
# ============================================================================


def print_colored(text, color="white", style="", emoji="", indent=0, animate=False):
    """Enhanced print colored text with optional animation"""
    if animate and VERBOSE_LOGGING:
        VisualEffects.print_with_animation(text, color, style, emoji, indent)
    else:
        indent_str = " " * indent
        emoji = emoji or Colors.get_random_emoji()
        style_tags = ""

        if style == "bold":
            style_tags = "bold "
        elif style == "italic":
            style_tags = "italic "
        elif style == "underline":
            style_tags = "underline "
        elif style == "blink":
            style_tags = "blink "

        console.print(
            f"{indent_str}{emoji} [{style_tags}{color}]{text}[/{style_tags}{color}]"
        )


def print_header(title, color=Colors.INFO, border_char="═", length=80, enhanced=True):
    """Print enhanced section header"""
    if enhanced:
        border = Colors.get_gradient_text(border_char * length, Colors.GOLD, color)
        console.print(border)

        title_text = Colors.get_gradient_text(f"   {title}   ", Colors.HIGHLIGHT, color)
        console.print(Align.center(title_text))

        console.print(border)
    else:
        border = border_char * length
        print_colored(border, color)
        print_colored(f"   {title}", color, "bold", "📌")
        print_colored(border, color)


def print_step(step_num, description, color=Colors.WARNING, animate=True):
    """Print enhanced step in the process"""
    emoji = Colors.get_random_emoji("trading")
    step_text = f"   STEP {step_num}: {description}"

    if animate:
        VisualEffects.print_with_animation(step_text, color, "bold", emoji, 0)
    else:
        print_colored(step_text, color, "bold", emoji)


def print_condition(condition, is_met=True, indent=2, enhanced=True):
    """Print enhanced condition with status"""
    if enhanced:
        if is_met:
            status = Colors.get_random_emoji("success")
            color = Colors.SUCCESS
            style = "bold"
        else:
            status = Colors.get_random_emoji("error")
            color = Colors.ERROR
            style = ""

        if is_met and PULSE_EFFECT:
            text = Colors.get_pulse_effect(f"{status} {condition}", color)
            console.print(" " * indent + text)
        else:
            print_colored(f"{status} {condition}", color, style, indent=indent)
    else:
        status = "✅" if is_met else "❌"
        color = Colors.SUCCESS if is_met else Colors.ERROR
        print_colored(f"{status} {condition}", color, indent=indent)


def print_waiting(waiting_for, indent=2, animate=True):
    """Print enhanced waiting status"""
    emoji = Colors.get_random_emoji("warning")

    if animate:
        pulse_text = Colors.get_pulse_effect(
            f"⏳ Waiting for: {waiting_for}", Colors.WARNING
        )
        console.print(" " * indent + emoji + " " + pulse_text)
    else:
        print_colored(
            f"⏳ Waiting for: {waiting_for}",
            Colors.WARNING,
            "italic",
            emoji,
            indent=indent,
        )


def print_signal(signal_type, description, color=Colors.MAGENTA, enhanced=True):
    """Print enhanced trading signal"""
    if enhanced:
        if signal_type in ["ENTRY CONFIRMED", "STOP LOSS TRIGGER", "EXIT SIGNAL"]:
            VisualEffects.flash_border(color, 60, 0.3)

        emoji = Colors.get_random_emoji("trading")
        signal_text = Colors.get_gradient_text(
            f"{signal_type}: {description}", color, Colors.HIGHLIGHT
        )
        console.print(f"\n{emoji} ", end="")
        console.print(signal_text)
    else:
        print_colored(f"\n🎯 {signal_type}: {description}", color, "bold")


def print_transition(from_state, to_state, color=Colors.INFO, enhanced=True):
    """Print enhanced state transition"""
    if enhanced:
        arrow_sequence = ["→", "⇒", "⇨", "⇾", "⟹"]

        console.print()
        for arrow in arrow_sequence:
            transition_text = f"[{Colors.WARNING}]{from_state}[/{Colors.WARNING}] [{color}]{arrow}[/{color}] [{Colors.SUCCESS}]{to_state}[/{Colors.SUCCESS}]"
            console.print(Align.center(Text(transition_text)), end="\r")
            time.sleep(0.1)

        final_arrow = "⇨"
        transition_text = f"[{Colors.WARNING}]{from_state}[/{Colors.WARNING}] [{color}]{final_arrow}[/{color}] [{Colors.SUCCESS}]{to_state}[/{Colors.SUCCESS}]"
        console.print(Align.center(Text(transition_text)))

        if (from_state == "BUY" and to_state == "SELL") or (
            from_state == "SELL" and to_state == "BUY"
        ):
            VisualEffects.flash_border(Colors.GOLD, 40, 0.2)
    else:
        print_colored(f"\n🔄 Transition: {from_state} → {to_state}", color, "bold")


# ============================================================================
# ENHANCED ART VISUALIZATIONS
# ============================================================================


def display_dino(message, dino_type="default", title="", enhanced=True):
    """Enhanced display ASCII art with message"""
    if enhanced:
        VisualEffects.display_animated_dino(message, dino_type, title)
    else:
        try:
            font = FONT_MAPPING.get(dino_type, "standard")
            dino = text2art(message, font=font)
            if title:
                print_header(title, Colors.INFO)
            console.print(dino)
        except Exception as e:
            print_colored(f"\n🦕 {message}\n", Colors.INFO)


def display_strategy_activation(daily_diff, enhanced=True):
    """Enhanced display strategy AAA activation with art"""
    if enhanced:
        title = "STRATEGY AAA ACTIVATED"
        range_desc = f"-1% to +4% (Current: {daily_diff:+.2f}%)"
        dino_type = "triceratops"
        color = Colors.STRATEGY_AAA
        art_message = "STRATEGY AAA"

        VisualEffects.display_animated_dino(art_message, dino_type, title)

        # Show progress bar for daily diff
        progress = (
            (daily_diff - DAILY_DIFF_LOWER_LIMIT)
            / (DAILY_DIFF_UPPER_LIMIT - DAILY_DIFF_LOWER_LIMIT)
        ) * 100
        progress = max(0, min(100, progress))
        progress_bar = VisualEffects.create_progress_bar(progress, 40, "Range:")
        print_colored(f"   {range_desc}", color, "bold")
        print_colored(f"   {progress_bar}", Colors.INFO)

        print_colored("   Following Strategy AAA flowchart exactly...", color, "bold")

        VisualEffects.display_spinner("Initializing strategy conditions", color)
    else:
        title = "STRATEGY AAA ACTIVATED"
        range_desc = "-1% to +4%"
        dino_type = "triceratops"
        color = Colors.SUCCESS

        display_dino(f"Daily Diff: {daily_diff:.2f}% ({range_desc})", dino_type, title)
        print_colored(f"Following Strategy AAA flowchart exactly...", color, "bold")


def display_mode_banner(mode, wallet_info, enhanced=True):
    """Enhanced display trading mode banner"""
    if enhanced:
        if mode == "BUY":
            title = "BUY MODE ACTIVATED"
            dino_type = "buy"
            color = "bright_green"
            action = "Looking for entry signals to BUY tokens"
            art_message = "BUY MODE"
        elif mode == "SELL":
            title = "SELL MODE ACTIVATED"
            dino_type = "sell"
            color = "bright_red"
            action = "Looking for entry signals to validate existing position"
            art_message = "SELL MODE"
        else:
            title = "NEUTRAL MODE"
            dino_type = "sleep"
            color = "bright_yellow"
            action = "Insufficient funds for trading"
            art_message = "NEUTRAL"

        VisualEffects.display_animated_dino(
            art_message, dino_type, title, border_style="heavy"
        )

        print_colored("💰 WALLET STATUS:", Colors.GOLD, "bold")

        wallet_table = Table(show_header=False, box=None, show_lines=False)
        wallet_table.add_column(style="cyan", width=15)
        wallet_table.add_column(style="bright_white")

        for asset, info in wallet_info.items():
            wallet_table.add_row(
                f"   • {asset}:", f"[bright_white]{info}[/bright_white]"
            )

        console.print(wallet_table)

        print_colored(f"\n🎯 ACTION: {action}", color, "bold")

        if mode == "SELL":
            print_colored(
                "   ⚡ In SELL mode: Will skip BUY when entry signal occurs",
                Colors.WARNING,
                "italic",
                Colors.get_random_emoji("warning"),
            )
            print_colored(
                "   ⚡ Existing tokens will be validated at entry signal",
                Colors.WARNING,
                "italic",
                Colors.get_random_emoji("warning"),
            )

            if "SOL" in str(wallet_info):
                print_colored(
                    "   📊 Position validation pending entry signal...",
                    Colors.INFO,
                    indent=2,
                )
    else:
        if mode == "BUY":
            title = "BUY MODE ACTIVATED"
            dino_type = "happy"
            color = Colors.SUCCESS
            action = "Looking for entry signals to BUY tokens"
        elif mode == "SELL":
            title = "SELL MODE ACTIVATED"
            dino_type = "raptor"
            color = Colors.ERROR
            action = "Looking for entry signals to validate existing position"
        else:
            title = "NEUTRAL MODE"
            dino_type = "sleep"
            color = Colors.WARNING
            action = "Insufficient funds for trading"

        display_dino(title, dino_type)

        print_colored("💰 WALLET STATUS:", color, "bold")
        for asset, info in wallet_info.items():
            print_colored(f"   • {asset}: {info}", Colors.WHITE)

        print_colored(f"\n🎯 ACTION: {action}", color, "bold")

        if mode == "SELL":
            print_colored(
                "   ⚡ In SELL mode: Will skip BUY when entry signal occurs",
                Colors.WARNING,
                "italic",
            )
            print_colored(
                "   ⚡ Existing tokens will be validated at entry signal",
                Colors.WARNING,
                "italic",
            )


def display_position_status(action, position_info, enhanced=True):
    """Enhanced display position opening/closing status"""
    if enhanced:
        if action == "OPENED":
            dino_type = "buy"
            color = Colors.SUCCESS
            title = "POSITION OPENED"
            art_message = "POSITION OPEN"
        else:
            dino_type = "sell"
            color = Colors.INFO
            title = "POSITION CLOSED"
            art_message = "POSITION CLOSED"

        VisualEffects.display_animated_dino(art_message, dino_type, title)

        pos_table = Table(
            show_header=True,
            header_style=f"bold {color}",
            box=HEAVY if action == "OPENED" else ROUNDED,
        )
        pos_table.add_column("Metric", style="cyan", width=20)
        pos_table.add_column("Value", style="bright_white")

        for key, value in position_info.items():
            pos_table.add_row(key, str(value))

        console.print(Panel(pos_table, title=f"📊 POSITION DETAILS", border_style=color))

        if action == "OPENED":
            VisualEffects.flash_border(Colors.GOLD, 40, 0.2)
    else:
        if action == "OPENED":
            dino_type = "happy"
            color = Colors.SUCCESS
            title = "POSITION OPENED"
        else:
            dino_type = "sleep"
            color = Colors.INFO
            title = "POSITION CLOSED"

        display_dino(title, dino_type)

        for key, value in position_info.items():
            print_colored(f"   • {key}: {value}", color)


def display_stop_loss_activation(enhanced=True):
    """Enhanced display stop loss activation"""
    if enhanced:
        title = "🚨 STOP LOSS ACTIVATED 🚨"
        dino_type = "alert"
        art_message = "STOP LOSS"

        VisualEffects.display_animated_dino(
            art_message, dino_type, title, border_style="heavy"
        )

        for _ in range(3):
            VisualEffects.flash_border(Colors.ERROR, 60, 0.2)
            time.sleep(0.1)

        print_colored(
            "MA_50 ≤ Fibo_23.6% - Activating stop loss flow", Colors.ERROR, "bold", "🚨"
        )

        VisualEffects.display_spinner("Activating stop loss protocol...", Colors.ERROR)
    else:
        display_dino("STOP LOSS TRIGGERED!", "trex_roar", "🚨 STOP LOSS ACTIVATED 🚨")
        print_colored(
            "MA_50 ≤ Fibo_23.6% - Activating stop loss flow", Colors.ERROR, "bold"
        )


# ============================================================================
# ENHANCED STATUS DISPLAY
# ============================================================================


def display_status(csv_monitor, enhanced=True):
    """Enhanced comprehensive status display"""
    csv_stats = csv_monitor.get_stats()

    try:
        ticker = client.get_symbol_ticker(symbol=SYMBOL)
        current_price = float(ticker["price"])
    except:
        current_price = 0.0

    if enhanced:
        console.print()

        header_text = Colors.get_gradient_text(
            "📊 ENHANCED TRADING STATUS", Colors.GOLD, Colors.HIGHLIGHT
        )
        console.print(Align.center(header_text))

        if TEST_MODE:
            test_badge = Text(" [TEST MODE] ", style="bold black on yellow")
            console.print(Align.center(test_badge))

        console.print()

        layout = Layout()

        top_table = Table(show_header=False, box=ROUNDED, show_lines=True)
        top_table.add_column(style="cyan", width=25)
        top_table.add_column(style="bright_white")
        top_table.add_column(style="cyan", width=25)
        top_table.add_column(style="bright_white")

        top_table.add_row(
            "Time",
            datetime.now().strftime("%H:%M:%S"),
            "Symbol",
            f"[bold]{SYMBOL}[/bold]",
        )
        top_table.add_row(
            "Price",
            f"[bright_green]${current_price:.4f}[/bright_green]",
            "Daily Diff",
            f"[{'bright_green' if trade_state.current_daily_diff >= 0 else 'bright_red'}]{trade_state.current_daily_diff:+.2f}%[/{'bright_green' if trade_state.current_daily_diff >= 0 else 'bright_red'}]",
        )

        csv_color = {
            "ACTIVE": "bright_green",
            "SLOW": "yellow",
            "STALE": "bright_red",
            "STALLED": "red",
            "ERROR": "red",
        }.get(csv_stats["status"], "white")

        csv_age = csv_stats["age"]
        age_color = (
            "bright_green"
            if csv_age < 60
            else "yellow"
            if csv_age < 300
            else "bright_red"
        )

        top_table.add_row(
            "Strategy",
            f"[bold {Colors.STRATEGY_AAA}]{trade_state.strategy_variant.value}[/bold {Colors.STRATEGY_AAA}]",
            "CSV Status",
            f"[{csv_color}]{csv_stats['status']}[/{csv_color}]",
        )
        top_table.add_row(
            "Phase",
            f"[bold {Colors.get_phase_color(trade_state.phase.value)}]{trade_state.phase.value}[/bold {Colors.get_phase_color(trade_state.phase.value)}]",
            "CSV Age",
            f"[{age_color}]{csv_age}s[/{age_color}]",
        )
        top_table.add_row(
            "Mode",
            f"[bold {Colors.get_mode_color(trade_state.mode.value)}]{trade_state.mode.value}[/bold {Colors.get_mode_color(trade_state.mode.value)}]",
            "CSV Rows",
            str(csv_stats["rows"]),
        )

        console.print(
            Panel(top_table, title="SYSTEM OVERVIEW", border_style=Colors.GOLD)
        )

        # Enhanced stats display
        stats_panel = Layout()
        stats_panel.split_row(Layout(name="left"), Layout(name="right"))

        stats_table = Table(show_header=False, box=SIMPLE, show_lines=True)
        stats_table.add_column(style="cyan", width=20)
        stats_table.add_column(style="bright_white")

        for k, v in trade_state.stats.items():
            stats_table.add_row(k.replace("_", " ").title(), str(v))

        stats_panel["left"].update(
            Panel(stats_table, title="📈 TRADING STATS", border_style=Colors.INFO)
        )

        # Waiting conditions
        wait_table = Table(show_header=False, box=SIMPLE, show_lines=True)
        wait_table.add_column(style="cyan", width=30)
        wait_table.add_column(style="bright_white")

        for k, v in trade_state.waiting_conditions.items():
            if v:
                wait_table.add_row(
                    k.replace("_", " ").title(),
                    f"[{'bright_green' if v else 'bright_red'}]{'YES' if v else 'NO'}[/{'bright_green' if v else 'bright_red'}]",
                )

        stats_panel["right"].update(
            Panel(wait_table, title="⏳ WAITING CONDITIONS", border_style=Colors.WARNING)
        )

        console.print(stats_panel)

        # Captured levels
        captured_table = Table(show_header=False, box=SIMPLE, show_lines=True)
        captured_table.add_column(style="cyan", width=20)
        captured_table.add_column(style="bright_white")

        captured_levels = {
            "Fibo_0": trade_state.captured_fibo_0,
            "Fibo_1": trade_state.captured_fibo_1,
            "Fibo_1_Dip": trade_state.captured_fibo_1_dip,
            "Fibo_0_Sell": trade_state.captured_fibo_0_sell,
        }

        for k, v in captured_levels.items():
            value = f"{v:.4f}" if v is not None else "None"
            captured_table.add_row(k, value)

        console.print(
            Panel(captured_table, title="🔒 CAPTURED LEVELS", border_style=Colors.ACCENT)
        )

        # Position info
        if trade_state.position_open:
            pos_info = {
                "Size": f"{trade_state.position_size:.6f} {TOKEN}",
                "Entry Price": f"${trade_state.entry_price:.4f}"
                if trade_state.entry_price
                else "N/A",
                "Virtual Entry": f"${trade_state.virtual_entry_price:.4f}"
                if trade_state.virtual_entry_price
                else "N/A",
                "Current Price": f"${current_price:.4f}",
            }
            pos_table = Table(show_header=False, box=SIMPLE, show_lines=True)
            pos_table.add_column(style="cyan", width=20)
            pos_table.add_column(style="bright_white")
            for k, v in pos_info.items():
                pos_table.add_row(k, v)
            console.print(
                Panel(
                    pos_table, title="📊 CURRENT POSITION", border_style=Colors.POSITION
                )
            )
    else:
        print_colored(f"Time: {datetime.now().strftime('%H:%M:%S')}", Colors.WHITE)
        print_colored(f"Symbol: {SYMBOL}", Colors.WHITE)
        print_colored(f"Price: ${current_price:.4f}", Colors.WHITE)
        print_colored(
            f"Daily Diff: {trade_state.current_daily_diff:+.2f}%", Colors.WHITE
        )
        print_colored(f"Strategy: {trade_state.strategy_variant.value}", Colors.WHITE)
        print_colored(f"Phase: {trade_state.phase.value}", Colors.WHITE)
        print_colored(f"Mode: {trade_state.mode.value}", Colors.WHITE)

        print_colored("\nStats:", Colors.INFO)
        for k, v in trade_state.stats.items():
            print_colored(f"  {k}: {v}", Colors.WHITE)

        print_colored("\nWaiting Conditions:", Colors.WARNING)
        for k, v in trade_state.waiting_conditions.items():
            if v:
                print_colored(f"  {k}: {v}", Colors.WHITE)

        print_colored("\nCaptured Levels:", Colors.ACCENT)
        print_colored(f"  Fibo_0: {trade_state.captured_fibo_0}", Colors.WHITE)
        print_colored(f"  Fibo_1: {trade_state.captured_fibo_1}", Colors.WHITE)
        print_colored(f"  Fibo_1_Dip: {trade_state.captured_fibo_1_dip}", Colors.WHITE)
        print_colored(
            f"  Fibo_0_Sell: {trade_state.captured_fibo_0_sell}", Colors.WHITE
        )

        print_colored("\nCSV Stats:", Colors.INFO)
        for k, v in csv_stats.items():
            print_colored(f"  {k}: {v}", Colors.WHITE)

        if trade_state.position_open:
            print_colored("\nPosition:", Colors.POSITION)
            print_colored(
                f"  Size: {trade_state.position_size:.6f} {TOKEN}", Colors.WHITE
            )
            print_colored(f"  Entry Price: {trade_state.entry_price}", Colors.WHITE)
            print_colored(
                f"  Virtual Entry: {trade_state.virtual_entry_price}", Colors.WHITE
            )


# ============================================================================
# ENUMS AND STATE MANAGEMENT
# ============================================================================


class TradingMode(Enum):
    BUY = "BUY"
    SELL = "SELL"
    NEUTRAL = "NEUTRAL"


class StrategyVariant(Enum):
    NONE = "NONE"
    AAA = "AAA"


class Phase(Enum):
    ENTRY_MONITORING = "ENTRY_MONITORING"
    ENTRY_SIGNAL_CONFIRMED = "ENTRY_SIGNAL_CONFIRMED"
    POSITION_OPEN = "POSITION_OPEN"
    EXIT_MONITORING = "EXIT_MONITORING"
    STOP_LOSS_ACTIVE = "STOP_LOSS_ACTIVE"


class StrategyType(Enum):
    MA_200_WAVE = "MA_200_WAVE"
    MA_350_WAVE = "MA_350_WAVE"
    MA_500_WAVE = "MA_500_WAVE"


class TradeState:
    def __init__(self):
        self.mode = TradingMode.NEUTRAL
        self.strategy_variant = StrategyVariant.NONE
        self.phase = Phase.ENTRY_MONITORING
        self.position_open = False
        self.position_size = 0.0
        self.entry_price = None
        self.virtual_entry_price = None
        self.active_strategy = None
        self.entry_signal_confirmed = False
        self.stats = {
            "entries": 0,
            "exits": 0,
            "stop_loss_triggers": 0,
            "profits": 0,
            "losses": 0,
        }
        self.captured_fibo_0 = None
        self.captured_fibo_1 = None
        self.captured_fibo_1_dip = None
        self.captured_fibo_0_sell = None
        # Enhanced waiting conditions for faithful flowchart implementation
        self.waiting_conditions = {
            # Entry waiting conditions
            "waiting_ma_200_above_fibo_236": False,
            "waiting_ma_350_above_fibo_236": False,
            "waiting_ma_500_above_fibo_236": False,
            # MA_200 Wave waiting conditions
            "waiting_ma_100_above_fibo_764": False,
            "waiting_ma_500_above_fibo_764": False,
            # MA_200 Wave Branch B continuation
            "waiting_ma_50_below_fibo_764": False,
            "waiting_rsi_ma50_above_50": False,  # Different from flowchart!
            # MA_350/500 Wave Branch B continuation quality check failed
            "waiting_rsi_ma60_first": False,
            "waiting_rsi_ma52_then": False,
            # MA_350/500 Wave Step 3 Path
            "waiting_ma_100_below_ma_500": False,
            "waiting_ma_100_above_new_fibo_1": False,
            "waiting_ma_350_above_new_fibo_1": False,
            "waiting_ma_50_below_fibo_764_step3": False,
            "waiting_rsi_ma55_first": False,
            "waiting_rsi_ma52_then_step3": False,
            # Stop Loss Conditions
            "waiting_rsi_ma55_for_condition1": False,
            "waiting_ma_50_below_ma_500_for_condition2": False,
            "waiting_rsi_ma55_first_for_condition2": False,
            "waiting_rsi_ma52_then_for_condition2": False,
            # Capture Path Dual Monitoring
            "capture_path_dual_monitoring_active": False,
            # Sell Mode
            "waiting_ma_200_below_fibo_0_sell": False,
        }
        self.last_processed_time = None
        self.current_daily_diff = 0.0
        self.wallet_mgr = None  # To be set later
        # New states for flowchart fidelity
        self.step_3_path_active = False
        self.branch_b_quality_check_passed = False
        self.capture_path_active = False
        self.condition2_active = False
        self.condition1_active = False
        self.dual_monitoring_active = False
        self.phase_2_check_active = False
        self.quality_check_passed = False


class WalletManager:
    def __init__(self, client):
        self.client = client

    def get_balance(self, asset):
        try:
            return float(self.client.get_asset_balance(asset=asset)["free"])
        except:
            return 0.0

    def get_token_value_usd(self, token):
        try:
            ticker = self.client.get_symbol_ticker(symbol=SYMBOL)
            price = float(ticker["price"])
            balance = self.get_balance(token)
            return balance * price
        except:
            return 0.0

    def determine_mode(self):
        # Prioritize SELL mode first
        if self.get_token_value_usd(TOKEN) >= MIN_TOKEN_VALUE_FOR_SELL:
            return TradingMode.SELL
        # Then check BUY mode, but only for FDUSD since pair is SOLFDUSD
        if self.get_balance("FDUSD") >= TRADE_AMOUNT_USD:
            return TradingMode.BUY
        return TradingMode.NEUTRAL


class CSVMonitor:
    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.last_mod_time = 0
        self.last_row_count = 0
        self.stale_counter = 0

    def check_update(self):
        try:
            current_mod_time = os.path.getmtime(self.csv_file)
            if current_mod_time != self.last_mod_time:
                self.last_mod_time = current_mod_time
                df = pd.read_csv(self.csv_file)
                self.last_row_count = len(df)
                self.stale_counter = 0
                return True
            else:
                self.stale_counter += CSV_UPDATE_CHECK_INTERVAL
                return False
        except Exception as e:
            logger.error(f"CSV check error: {e}")
            return False

    def get_latest_rows(self, batch_size=1):
        try:
            df = pd.read_csv(self.csv_file)
            return df.tail(batch_size)
        except Exception as e:
            logger.error(f"CSV read error: {e}")
            return pd.DataFrame()

    def get_stats(self):
        status = "ACTIVE" if self.stale_counter < CSV_STALE_THRESHOLD else "STALE"
        return {
            "status": status,
            "age": self.stale_counter,
            "rows": self.last_row_count,
        }


class SafetyManager:
    def __init__(self):
        self.last_order_time = 0

    def can_place_order(self):
        return time.time() - self.last_order_time > ORDER_COOLDOWN_SECONDS

    def update_last_order_time(self):
        self.last_order_time = time.time()


# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO if VERBOSE_LOGGING else logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ============================================================================
# SIGNAL HANDLING
# ============================================================================


def signal_handler(sig, frame):
    global running
    display_dino("SHUTDOWN", "shutdown", "SYSTEM SHUTDOWN", enhanced=True)
    print_colored("Saving state and exiting...", Colors.WARNING, "bold")
    save_state()
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ============================================================================
# CORE TRADING FUNCTIONS
# ============================================================================


def calculate_daily_diff(daily_diff_str):
    try:
        return float(daily_diff_str.strip("%"))
    except:
        return 0.0


def check_entry_setup(row):
    fibo_236 = row.get("level_236")
    if pd.isna(fibo_236):
        return None

    ma_200 = row.get("long200")
    ma_350 = row.get("long350")
    ma_500 = row.get("long500")
    ma_100 = row.get("long100")
    ma_50 = row.get("short50")
    ma_21 = row.get("short21")
    ma_7 = row.get("short007")
    ma_2 = row.get("short002")

    all_mas = [ma_2, ma_7, ma_21, ma_50, ma_100, ma_350, ma_500]

    if (
        all(not pd.isna(ma) and ma >= fibo_236 for ma in all_mas)
        and not pd.isna(ma_200)
        and ma_200 <= fibo_236
    ):
        return StrategyType.MA_200_WAVE

    all_mas_350 = [ma_2, ma_7, ma_21, ma_50, ma_100, ma_200, ma_500]
    if (
        all(not pd.isna(ma) and ma >= fibo_236 for ma in all_mas_350)
        and not pd.isna(ma_350)
        and ma_350 <= fibo_236
    ):
        return StrategyType.MA_350_WAVE

    all_mas_500 = [ma_2, ma_7, ma_21, ma_50, ma_100, ma_200, ma_350]
    if (
        all(not pd.isna(ma) and ma >= fibo_236 for ma in all_mas_500)
        and not pd.isna(ma_500)
        and ma_500 <= fibo_236
    ):
        return StrategyType.MA_500_WAVE

    return None


def is_entry_condition_met(row, entry_setup):
    fibo_236 = row.get("level_236")
    if pd.isna(fibo_236):
        return False

    ma_2 = row.get("short002")
    ma_7 = row.get("short007")
    ma_21 = row.get("short21")
    ma_50 = row.get("short50")
    ma_100 = row.get("long100")
    ma_200 = row.get("long200")
    ma_350 = row.get("long350")
    ma_500 = row.get("long500")

    all_mas = [ma_2, ma_7, ma_21, ma_50, ma_100, ma_200, ma_350, ma_500]

    return all(not pd.isna(ma) and ma >= fibo_236 for ma in all_mas)


def process_entry_signal(row, entry_setup):
    with state_lock:
        trade_state.entry_signal_confirmed = True
        trade_state.active_strategy = entry_setup
        trade_state.phase = Phase.ENTRY_SIGNAL_CONFIRMED
        trade_state.stats["entries"] += 1

    print_transition("ENTRY MONITORING", "ENTRY CONFIRMED", Colors.SUCCESS)

    if trade_state.mode == TradingMode.BUY:
        order_result = order_executor.execute_market_order(
            "BUY", TRADE_AMOUNT_USD, TEST_MODE
        )
        if order_result["status"] in ["SUCCESS", "TEST_SUCCESS"]:
            order = order_result["order"]
            entry_price = float(order["fills"][0]["price"]) if "fills" in order else 0.0
            position_size = (
                float(order["executedQty"]) if "executedQty" in order else 0.0
            )

            with state_lock:
                trade_state.position_open = True
                trade_state.position_size = position_size
                trade_state.entry_price = entry_price
                trade_state.phase = Phase.POSITION_OPEN

            log_binance_transaction(order, "ENTRY", trade_state.strategy_variant.value)
            display_position_status(
                "OPENED",
                {
                    "Strategy": entry_setup.value,
                    "Size": f"{position_size:.6f} {TOKEN}",
                    "Price": f"${entry_price:.4f}",
                },
            )
            print_transition("ENTRY CONFIRMED", "POSITION OPEN", Colors.POSITION)
        else:
            print_colored("Entry order failed!", Colors.ERROR, "bold")
            with state_lock:
                trade_state.entry_signal_confirmed = False
                trade_state.phase = Phase.ENTRY_MONITORING
    elif trade_state.mode == TradingMode.SELL:
        try:
            ticker = client.get_symbol_ticker(symbol=SYMBOL)
            virtual_entry_price = float(ticker["price"])
        except:
            virtual_entry_price = 0.0

        with state_lock:
            trade_state.position_open = True
            trade_state.virtual_entry_price = virtual_entry_price
            trade_state.phase = Phase.POSITION_OPEN

        print_colored("SELL MODE: Position validated virtually", Colors.WARNING, "bold")
        display_position_status(
            "VALIDATED",
            {
                "Strategy": entry_setup.value,
                "Virtual Entry": f"${virtual_entry_price:.4f}",
            },
        )
        print_transition("ENTRY CONFIRMED", "POSITION VALIDATED", Colors.POSITION)


def monitor_strategy_aaa_exit(row):
    if trade_state.active_strategy == StrategyType.MA_200_WAVE:
        monitor_ma_200_wave_exit(row)
    elif trade_state.active_strategy in [
        StrategyType.MA_350_WAVE,
        StrategyType.MA_500_WAVE,
    ]:
        monitor_ma_350_500_wave_exit(row)


def monitor_ma_200_wave_exit(row):
    """Faithful implementation of MA_200 Wave Exit Monitoring from flowchart"""
    print_header("MA_200 WAVE EXIT MONITORING", Colors.INFO)

    fibo_764 = row.get("level_764")
    fibo_236 = row.get("level_236")
    ma_100 = row.get("long100")
    ma_50 = row.get("short50")
    ma_500 = row.get("long500")
    rsi_ma50 = row.get("rsi_ma50")

    if any(pd.isna(x) for x in [fibo_764, fibo_236, ma_100, ma_50, ma_500, rsi_ma50]):
        return

    # Reset dual monitoring flag if not active
    if not trade_state.dual_monitoring_active:
        trade_state.dual_monitoring_active = True
        print_step("DUAL", "Starting dual monitoring (two branches)", Colors.WARNING)

    # BRANCH A (REVERSAL): Check MA_50 ≤ Fibo_0.236
    branch_a_condition = ma_50 <= fibo_236
    print_condition(
        f"BRANCH A (Reversal): MA_50 ({ma_50:.4f}) ≤ Fibo_23.6% ({fibo_236:.4f})",
        branch_a_condition,
    )

    # BRANCH B (CONTINUATION): Check MA_100 ≥ Captured Fibo_1
    captured_fibo_1 = trade_state.captured_fibo_1
    branch_b_condition = False
    if captured_fibo_1 is not None:
        branch_b_condition = ma_100 >= captured_fibo_1
        print_condition(
            f"BRANCH B (Continuation): MA_100 ({ma_100:.4f}) ≥ Captured Fibo_1 ({captured_fibo_1:.4f})",
            branch_b_condition,
        )
    else:
        print_condition("BRANCH B (Continuation): No captured Fibo_1 yet", False)

    # Check for pre-conditions before dual monitoring
    if captured_fibo_1 is None:
        # Need to wait for both conditions first
        if (
            not trade_state.waiting_conditions["waiting_ma_100_above_fibo_764"]
            or not trade_state.waiting_conditions["waiting_ma_500_above_fibo_764"]
        ):
            ma100_ge_764 = ma_100 >= fibo_764
            ma500_ge_764 = ma_500 >= fibo_764

            print_condition(f"MA_100 ≥ Fibo_76.4% ({fibo_764:.4f})", ma100_ge_764)
            print_condition(f"MA_500 ≥ Fibo_76.4% ({fibo_764:.4f})", ma500_ge_764)

            if ma100_ge_764 and ma500_ge_764:
                trade_state.captured_fibo_1 = ma_100
                print_step(
                    "CAPTURE",
                    f"Fibo_1 captured at MA_100: {trade_state.captured_fibo_1:.4f}",
                    Colors.INFO,
                )
                trade_state.phase = Phase.EXIT_MONITORING
                print_transition("POSITION OPEN", "EXIT MONITORING", Colors.EXIT)
                return  # Return to let next iteration handle dual monitoring
            else:
                print_waiting("BOTH MA_100 AND MA_500 ≥ Fibo_76.4%")
                return
        return

    # DUAL MONITORING - First wins logic
    if branch_a_condition:
        print_signal(
            "BRANCH A TRIGGERED",
            "MA_50 ≤ Fibo_23.6% - Activating stop loss",
            Colors.ERROR,
        )
        display_stop_loss_activation()
        with state_lock:
            trade_state.captured_fibo_0 = row.get("level_000")
            trade_state.phase = Phase.STOP_LOSS_ACTIVE
            trade_state.dual_monitoring_active = False
        trade_state.stats["stop_loss_triggers"] += 1
        execute_stop_loss_flow(row)
    elif branch_b_condition:
        print_signal(
            "BRANCH B TRIGGERED",
            "MA_100 ≥ Captured Fibo_1 - Continuation path",
            Colors.INFO,
        )

        if not trade_state.waiting_conditions["waiting_ma_50_below_fibo_764"]:
            ma50_le_764 = ma_50 <= fibo_764
            print_condition(f"MA_50 ≤ Fibo_76.4% ({fibo_764:.4f})", ma50_le_764)
            if ma50_le_764:
                trade_state.waiting_conditions["waiting_ma_50_below_fibo_764"] = True
                print_condition("MA_50 ≤ Fibo_76.4% - Condition met", True)
            else:
                print_waiting("MA_50 ≤ Fibo_76.4%")
                return

        # FAITHFUL: Wait for RSI_MA50 ≥ 55 (not 50 as in previous code)
        if not trade_state.waiting_conditions["waiting_rsi_ma55_first"]:
            if rsi_ma50 >= 55:
                trade_state.waiting_conditions["waiting_rsi_ma55_first"] = True
                print_condition("RSI_MA50 ≥ 55 (first)", True)
                print_waiting("RSI_MA50 ≤ 52 (then)")
            else:
                print_waiting("RSI_MA50 ≥ 55 (first)")
                return
        else:
            if rsi_ma50 <= 52:
                print_condition("RSI_MA50 ≤ 52 (then)", True)
                execute_exit_order(row, "MA_200_WAVE_CONTINUATION_EXIT")
            else:
                print_waiting("RSI_MA50 ≤ 52 (then)")
    else:
        print_waiting(
            "BRANCH A (MA_50 ≤ Fibo_23.6%) OR BRANCH B (MA_100 ≥ Captured Fibo_1)"
        )


def monitor_ma_350_500_wave_exit(row):
    """Faithful implementation of MA_350/500 Wave Exit Monitoring from flowchart"""
    print_header("MA_350/500 WAVE EXIT MONITORING", Colors.INFO)

    fibo_764 = row.get("level_764")
    fibo_236 = row.get("level_236")
    ma_100 = row.get("long100")
    ma_50 = row.get("short50")
    ma_500 = row.get("long500")
    ma_200 = row.get("long200")
    ma_350 = row.get("long350")
    rsi_ma50 = row.get("rsi_ma50")

    if any(
        pd.isna(x)
        for x in [fibo_764, fibo_236, ma_100, ma_50, ma_500, ma_200, ma_350, rsi_ma50]
    ):
        return

    # Reset dual monitoring flag if not active
    if not trade_state.dual_monitoring_active:
        trade_state.dual_monitoring_active = True
        print_step("DUAL", "Starting dual monitoring (two branches)", Colors.WARNING)

    # BRANCH A (REVERSAL): Check MA_50 ≤ Fibo_0.236
    branch_a_condition = ma_50 <= fibo_236
    print_condition(
        f"BRANCH A (Reversal): MA_50 ({ma_50:.4f}) ≤ Fibo_23.6% ({fibo_236:.4f})",
        branch_a_condition,
    )

    # BRANCH B (CONTINUATION): Check MA_200 ≥ Fibo 0.764
    branch_b_condition = ma_200 >= fibo_764
    print_condition(
        f"BRANCH B (Continuation): MA_200 ({ma_200:.4f}) ≥ Fibo_76.4% ({fibo_764:.4f})",
        branch_b_condition,
    )

    # DUAL MONITORING - First wins logic
    if branch_a_condition:
        print_signal(
            "BRANCH A TRIGGERED",
            "MA_50 ≤ Fibo_23.6% - Activating stop loss",
            Colors.ERROR,
        )
        display_stop_loss_activation()
        with state_lock:
            trade_state.captured_fibo_0 = row.get("level_000")
            trade_state.phase = Phase.STOP_LOSS_ACTIVE
            trade_state.dual_monitoring_active = False
        trade_state.stats["stop_loss_triggers"] += 1
        execute_stop_loss_flow(row)
    elif branch_b_condition:
        print_signal(
            "BRANCH B TRIGGERED",
            "MA_200 ≥ Fibo_76.4% - Checking quality",
            Colors.INFO,
        )

        # QUALITY CHECK: Are BOTH MA_350 ≤ MA_200 AND MA_500 ≤ MA_200?
        quality_check_1 = ma_350 <= ma_200
        quality_check_2 = ma_500 <= ma_200
        both_conditions_met = quality_check_1 and quality_check_2

        print_condition(
            f"Quality Check 1: MA_350 ({ma_350:.4f}) ≤ MA_200 ({ma_200:.4f})",
            quality_check_1,
        )
        print_condition(
            f"Quality Check 2: MA_500 ({ma_500:.4f}) ≤ MA_200 ({ma_200:.4f})",
            quality_check_2,
        )

        if both_conditions_met:
            print_step(
                "QUALITY", "Both conditions met - Proceeding to Phase 2", Colors.SUCCESS
            )
            trade_state.quality_check_passed = True
            trade_state.phase_2_check_active = True
            monitor_phase_2_check(row)
        else:
            print_step(
                "QUALITY", "Quality check failed - Waiting for RSI", Colors.WARNING
            )
            trade_state.quality_check_passed = False
            monitor_quality_check_failed_path(row)
    else:
        print_waiting("BRANCH A (MA_50 ≤ Fibo_23.6%) OR BRANCH B (MA_200 ≥ Fibo_76.4%)")


def monitor_phase_2_check(row):
    """Phase 2: Check ALL lesser MAs (002, 007, 21, 50, 100) ≥ MA_200"""
    print_step("PHASE 2", "Checking all lesser MAs ≥ MA_200", Colors.INFO)

    ma_200 = row.get("long200")
    if pd.isna(ma_200):
        return

    lesser_mas = [
        ("MA_002", row.get("short002")),
        ("MA_007", row.get("short007")),
        ("MA_021", row.get("short21")),
        ("MA_050", row.get("short50")),
        ("MA_100", row.get("long100")),
    ]

    all_lesser_ge_ma200 = True
    for ma_name, ma_value in lesser_mas:
        if pd.isna(ma_value):
            print_condition(f"{ma_name}: N/A", False)
            all_lesser_ge_ma200 = False
        else:
            condition_met = ma_value >= ma_200
            print_condition(
                f"{ma_name} ({ma_value:.4f}) ≥ MA_200 ({ma_200:.4f})",
                condition_met,
            )
            if not condition_met:
                all_lesser_ge_ma200 = False

    if all_lesser_ge_ma200:
        print_step(
            "STEP 3", "All conditions met - Proceeding to Step 3", Colors.SUCCESS
        )
        trade_state.step_3_path_active = True
        trade_state.phase_2_check_active = False
        monitor_step_3_path(row)
    else:
        print_step(
            "CAPTURE PATH",
            "Some lesser MAs < MA_200 - Entering Capture Path",
            Colors.WARNING,
        )
        trade_state.phase_2_check_active = False
        trade_state.capture_path_active = True
        monitor_capture_path(row)


def monitor_step_3_path(row):
    """Step 3 Path: Wait for MA_100 ≤ MA_500, then capture new Fibo_1, etc."""
    print_step("STEP 3 PATH", "Following Step 3 path from flowchart", Colors.INFO)

    ma_100 = row.get("long100")
    ma_500 = row.get("long500")
    fibo_764 = row.get("level_764")

    if any(pd.isna(x) for x in [ma_100, ma_500, fibo_764]):
        return

    # Wait for: MA_100 ≤ MA_500
    if not trade_state.waiting_conditions["waiting_ma_100_below_ma_500"]:
        ma_100_le_ma_500 = ma_100 <= ma_500
        print_condition(
            f"MA_100 ({ma_100:.4f}) ≤ MA_500 ({ma_500:.4f})", ma_100_le_ma_500
        )

        if ma_100_le_ma_500:
            trade_state.waiting_conditions["waiting_ma_100_below_ma_500"] = True
            # CAPTURE NEW Fibo_1 at MA_100 dip position
            trade_state.captured_fibo_1 = ma_100
            print_step(
                "CAPTURE NEW",
                f"New Fibo_1 at MA_100 dip: {trade_state.captured_fibo_1:.4f}",
                Colors.INFO,
            )
        else:
            print_waiting("MA_100 ≤ MA_500")
            return

    # Check which branch based on MA_500
    ma500_ge_764 = ma_500 >= fibo_764
    print_condition(
        f"MA_500 ({ma_500:.4f}) ≥ Fibo_76.4% ({fibo_764:.4f})", ma500_ge_764
    )

    if ma500_ge_764:
        print_step("BRANCH A", "MA_500 ≥ Fibo_76.4% - Branch A", Colors.INFO)
        # Wait for: MA_100 ≥ New Fibo_1
        if not trade_state.waiting_conditions["waiting_ma_100_above_new_fibo_1"]:
            ma100_ge_new_fibo1 = ma_100 >= trade_state.captured_fibo_1
            print_condition(
                f"MA_100 ({ma_100:.4f}) ≥ New Fibo_1 ({trade_state.captured_fibo_1:.4f})",
                ma100_ge_new_fibo1,
            )
            if ma100_ge_new_fibo1:
                trade_state.waiting_conditions["waiting_ma_100_above_new_fibo_1"] = True
            else:
                print_waiting(
                    f"MA_100 ≥ New Fibo_1 ({trade_state.captured_fibo_1:.4f})"
                )
                return
    else:
        print_step("BRANCH B", "MA_500 ≤ Fibo_76.4% - Branch B", Colors.INFO)
        # Wait for: MA_350 ≥ New Fibo_1
        ma_350 = row.get("long350")
        if pd.isna(ma_350):
            return

        if not trade_state.waiting_conditions["waiting_ma_350_above_new_fibo_1"]:
            ma350_ge_new_fibo1 = ma_350 >= trade_state.captured_fibo_1
            print_condition(
                f"MA_350 ({ma_350:.4f}) ≥ New Fibo_1 ({trade_state.captured_fibo_1:.4f})",
                ma350_ge_new_fibo1,
            )
            if ma350_ge_new_fibo1:
                trade_state.waiting_conditions["waiting_ma_350_above_new_fibo_1"] = True
            else:
                print_waiting(
                    f"MA_350 ≥ New Fibo_1 ({trade_state.captured_fibo_1:.4f})"
                )
                return

    # Common path after branch-specific condition
    monitor_step_3_final_exit(row)


def monitor_step_3_final_exit(row):
    """Final exit conditions for Step 3 Path"""
    print_step("STEP 3 FINAL", "Waiting for final exit conditions", Colors.INFO)

    fibo_764 = row.get("level_764")
    ma_50 = row.get("short50")
    rsi_ma50 = row.get("rsi_ma50")

    if any(pd.isna(x) for x in [fibo_764, ma_50, rsi_ma50]):
        return

    # Wait for: MA_50 ≤ Fibo_76.4%
    if not trade_state.waiting_conditions["waiting_ma_50_below_fibo_764_step3"]:
        ma50_le_764 = ma_50 <= fibo_764
        print_condition(
            f"MA_50 ({ma_50:.4f}) ≤ Fibo_76.4% ({fibo_764:.4f})", ma50_le_764
        )

        if ma50_le_764:
            trade_state.waiting_conditions["waiting_ma_50_below_fibo_764_step3"] = True
        else:
            print_waiting(f"MA_50 ≤ Fibo_76.4% ({fibo_764:.4f})")
            return

    # Wait for RSI sequence: First ≥ 55, THEN ≤ 52
    if not trade_state.waiting_conditions["waiting_rsi_ma55_first"]:
        if rsi_ma50 >= 55:
            trade_state.waiting_conditions["waiting_rsi_ma55_first"] = True
            print_condition("RSI_MA50 ≥ 55 (first)", True)
            print_waiting("RSI_MA50 ≤ 52 (then)")
        else:
            print_waiting("RSI_MA50 ≥ 55 (first)")
            return
    else:
        if rsi_ma50 <= 52:
            print_condition("RSI_MA50 ≤ 52 (then)", True)
            execute_exit_order(row, "STRATEGY_AAA_STEP_3_EXIT")
        else:
            print_waiting("RSI_MA50 ≤ 52 (then)")


def monitor_capture_path(row):
    """Capture Path: CAPTURE Fibo_1 level (level_100), then dual monitoring"""
    print_step("CAPTURE PATH", "Entering Capture Path", Colors.WARNING)

    # CAPTURE Fibo_1 level (level_100)
    fibo_1 = row.get("level_100")
    if pd.isna(fibo_1):
        print_waiting("Valid level_100 for capture")
        return

    trade_state.captured_fibo_1_dip = fibo_1
    print_step(
        "CAPTURE",
        f"Captured Fibo_1 at dip: {trade_state.captured_fibo_1_dip:.4f}",
        Colors.INFO,
    )

    # INITIATE DUAL MONITORING
    trade_state.capture_path_active = True
    initiate_capture_dual_monitoring(row)


def initiate_capture_dual_monitoring(row):
    """Dual monitoring for capture path: Listen for TWO SPECIFIC CONDITIONS"""
    print_step(
        "CAPTURE DUAL",
        "Initiating dual monitoring for two conditions",
        Colors.WARNING,
    )

    captured_fibo_1 = trade_state.captured_fibo_1_dip or trade_state.captured_fibo_1
    if captured_fibo_1 is None:
        return

    ma_100 = row.get("long100")
    fibo_236 = row.get("level_236")
    ma_50 = row.get("short50")

    if any(pd.isna(x) for x in [ma_100, fibo_236, ma_50]):
        return

    print_condition(
        f"CONDITION 1: MA_100 ({ma_100:.4f}) ≥ Captured Fibo_1 ({captured_fibo_1:.4f})",
        ma_100 >= captured_fibo_1,
    )
    print_condition(
        f"CONDITION 2: MA_50 ({ma_50:.4f}) ≤ Fibo_23.6% ({fibo_236:.4f})",
        ma_50 <= fibo_236,
    )

    # Check which condition is met first (FIRST WINS logic)
    condition1_met = ma_100 >= captured_fibo_1
    condition2_met = ma_50 <= fibo_236

    if condition1_met:
        print_signal(
            "CAPTURE PATH EXIT",
            "CONDITION 1 met first - MA_100 ≥ Captured Fibo_1",
            Colors.INFO,
        )
        execute_exit_order(row, "CAPTURE_PATH_MA100_EXIT")
    elif condition2_met:
        print_signal(
            "STOP LOSS TRIGGER",
            "CONDITION 2 met first - MA_50 ≤ Fibo_23.6%",
            Colors.ERROR,
        )
        display_stop_loss_activation()
        with state_lock:
            trade_state.captured_fibo_0 = row.get("level_000")
            trade_state.phase = Phase.STOP_LOSS_ACTIVE
            trade_state.capture_path_active = False
        trade_state.stats["stop_loss_triggers"] += 1
        execute_stop_loss_flow(row)
    else:
        print_waiting(
            "CONDITION 1 (MA_100 ≥ Captured Fibo_1) OR CONDITION 2 (MA_50 ≤ Fibo_23.6%)"
        )


def monitor_quality_check_failed_path(row):
    """Path when quality check fails: Wait for RSI sequence"""
    print_step("QUALITY FAIL", "Quality check failed path", Colors.WARNING)

    rsi_ma50 = row.get("rsi_ma50")
    if pd.isna(rsi_ma50):
        return

    # FAITHFUL: Wait for RSI_MA50 ≥ 60 (not 55 as in MA_200 wave)
    if not trade_state.waiting_conditions["waiting_rsi_ma60_first"]:
        if rsi_ma50 >= 60:
            trade_state.waiting_conditions["waiting_rsi_ma60_first"] = True
            print_condition("RSI_MA50 ≥ 60 (first)", True)
            print_waiting("RSI_MA50 ≤ 52 (then)")
        else:
            print_waiting("RSI_MA50 ≥ 60 (first)")
    else:
        if rsi_ma50 <= 52:
            print_condition("RSI_MA50 ≤ 52 (then)", True)
            execute_exit_order(row, "QUALITY_FAIL_RSI_EXIT")
        else:
            print_waiting("RSI_MA50 ≤ 52 (then)")


def execute_stop_loss_flow(row):
    """FAITHFUL implementation of Stop Loss Flow from flowchart"""
    print_header("STOP LOSS FLOW", Colors.STOP_LOSS)

    captured_fibo_0 = trade_state.captured_fibo_0
    if captured_fibo_0 is None:
        print_waiting("Captured Fibo_0 not set")
        return

    ma_50 = row.get("short50")
    ma_100 = row.get("long100")
    fibo_50 = row.get("level_500")
    ma_500 = row.get("long500")
    rsi_ma50 = row.get("rsi_ma50")

    if any(pd.isna(x) for x in [ma_50, ma_100, fibo_50, ma_500, rsi_ma50]):
        return

    # Reset condition flags
    if not trade_state.condition1_active and not trade_state.condition2_active:
        trade_state.condition1_active = True
        trade_state.condition2_active = True
        print_step("PARALLEL", "Checking two conditions in parallel", Colors.WARNING)

    # CONDITION 1: MA_50 ≤ captured Fibo_0
    condition1_met = ma_50 <= captured_fibo_0
    print_condition(
        f"CONDITION 1: MA_50 ({ma_50:.4f}) ≤ Captured Fibo_0 ({captured_fibo_0:.4f})",
        condition1_met,
    )

    if condition1_met and trade_state.condition1_active:
        print_step("CONDITION 1", "Condition 1 met - Waiting for RSI", Colors.INFO)
        # Wait for: rsi_ma50 ≥ 55
        if not trade_state.waiting_conditions["waiting_rsi_ma55_for_condition1"]:
            if rsi_ma50 >= 55:
                trade_state.waiting_conditions["waiting_rsi_ma55_for_condition1"] = True
                print_condition("RSI_MA50 ≥ 55", True)
                execute_exit_order(row, "STOP_LOSS_CONDITION_1")
                trade_state.condition1_active = False
            else:
                print_waiting("RSI_MA50 ≥ 55 for Condition 1")
        return  # Return early if condition 1 is being processed

    # CONDITION 2: MA_100 ≥ Fibo_50%
    condition2_met = ma_100 >= fibo_50
    print_condition(
        f"CONDITION 2: MA_100 ({ma_100:.4f}) ≥ Fibo_50% ({fibo_50:.4f})",
        condition2_met,
    )

    if condition2_met and trade_state.condition2_active:
        print_step(
            "CONDITION 2", "Condition 2 met - Checking MA_50 ≤ MA_500", Colors.INFO
        )

        # CHECK: MA_50 ≤ MA_500
        ma50_le_ma500 = ma_50 <= ma_500
        print_condition(f"MA_50 ({ma_50:.4f}) ≤ MA_500 ({ma_500:.4f})", ma50_le_ma500)

        if ma50_le_ma500:
            # Wait for RSI sequence: First ≥ 55, THEN ≤ 52
            if not trade_state.waiting_conditions[
                "waiting_rsi_ma55_first_for_condition2"
            ]:
                if rsi_ma50 >= 55:
                    trade_state.waiting_conditions[
                        "waiting_rsi_ma55_first_for_condition2"
                    ] = True
                    print_condition("RSI_MA50 ≥ 55 (first)", True)
                    print_waiting("RSI_MA50 ≤ 52 (then)")
                else:
                    print_waiting("RSI_MA50 ≥ 55 (first)")
            else:
                if rsi_ma50 <= 52:
                    print_condition("RSI_MA50 ≤ 52 (then)", True)
                    execute_exit_order(row, "STOP_LOSS_CONDITION_2")
                    trade_state.condition2_active = False
                else:
                    print_waiting("RSI_MA50 ≤ 52 (then)")
        else:
            print_waiting("MA_50 ≤ MA_500 for Condition 2")
    elif not condition1_met and not condition2_met:
        print_waiting("Condition 1 OR Condition 2 to be met")


def execute_exit_order(row, exit_reason):
    """Execute exit order with comprehensive cleanup"""
    print_signal("EXIT SIGNAL", f"Executing exit: {exit_reason}", Colors.EXIT)

    if trade_state.mode == TradingMode.SELL and trade_state.position_open:
        token_amount = trade_state.wallet_mgr.get_balance(TOKEN)
        if token_amount > 0:
            order_result = order_executor.execute_market_order(
                "SELL", token_amount, TEST_MODE
            )
            if order_result["status"] in ["SUCCESS", "TEST_SUCCESS"]:
                order = order_result["order"]
                exit_price = (
                    float(order["fills"][0]["price"]) if "fills" in order else 0.0
                )
                pnl_usd = (
                    exit_price
                    - (trade_state.entry_price or trade_state.virtual_entry_price or 0)
                ) * trade_state.position_size
                pnl_percent = (
                    pnl_usd
                    / (trade_state.entry_price or trade_state.virtual_entry_price or 1)
                ) * 100

                log_binance_transaction(
                    order,
                    "EXIT",
                    trade_state.strategy_variant.value,
                    pnl_usd=pnl_usd,
                    pnl_percent=pnl_percent,
                    exit_reason=exit_reason,
                )

                if pnl_usd > 0:
                    trade_state.stats["profits"] += 1
                else:
                    trade_state.stats["losses"] += 1

                trade_state.stats["exits"] += 1

                display_position_status(
                    "CLOSED",
                    {
                        "Reason": exit_reason,
                        "PnL USD": f"${pnl_usd:.2f}",
                        "PnL %": f"{pnl_percent:.2f}%",
                    },
                )
            else:
                print_colored("Exit order failed!", Colors.ERROR, "bold")
                return
        else:
            print_colored("No tokens to sell", Colors.WARNING)

    # Comprehensive state reset
    with state_lock:
        trade_state.position_open = False
        trade_state.entry_signal_confirmed = False
        trade_state.phase = Phase.ENTRY_MONITORING
        trade_state.active_strategy = None
        trade_state.captured_fibo_0 = None
        trade_state.captured_fibo_1 = None
        trade_state.captured_fibo_1_dip = None
        trade_state.captured_fibo_0_sell = None

        # Reset all flowchart-specific flags
        trade_state.step_3_path_active = False
        trade_state.branch_b_quality_check_passed = False
        trade_state.capture_path_active = False
        trade_state.condition2_active = False
        trade_state.condition1_active = False
        trade_state.dual_monitoring_active = False
        trade_state.phase_2_check_active = False
        trade_state.quality_check_passed = False

        # Reset all waiting conditions
        for key in trade_state.waiting_conditions:
            trade_state.waiting_conditions[key] = False

    print_transition("POSITION OPEN", "ENTRY MONITORING", Colors.ENTRY)
    update_trading_mode()


# ============================================================================
# ORDER EXECUTOR
# ============================================================================


class OrderExecutor:
    def __init__(self, client, safety_mgr, wallet_mgr):
        self.client = client
        self.safety_mgr = safety_mgr
        self.wallet_mgr = wallet_mgr

    def execute_market_order(self, side, token_amount, is_test=False):
        if not self.safety_mgr.can_place_order():
            return {"status": "COOLDOWN", "error": "Order cooldown active"}
        print_colored(f"\n{'='*60}", Colors.INFO)
        print_colored(
            f"🎯 {'TEST' if is_test else 'LIVE'} {side} ORDER",
            Colors.SUCCESS if side == "BUY" else Colors.ERROR,
            "bold",
        )
        print_colored(f"📊 Amount: {token_amount:.6f} {TOKEN}", Colors.INFO)
        print_colored(f"{'='*60}", Colors.INFO)
        try:
            ticker = self.client.get_symbol_ticker(symbol=SYMBOL)
            current_price = float(ticker["price"])
            if is_test:
                return self._execute_test_order(side, token_amount, current_price)
            else:
                return self._execute_real_order(side, token_amount)
        except Exception as e:
            error_msg = f"Order error: {e}"
            print_colored(error_msg, Colors.ERROR, "bold")
            return {"status": "ERROR", "error": str(e)}
        finally:
            self.safety_mgr.update_last_order_time()

    def _execute_real_order(self, side, token_amount):
        try:
            quantity_str = format(token_amount, "f").rstrip("0").rstrip(".")
            if side == "BUY":
                order = self.client.order_market_buy(
                    symbol=SYMBOL,
                    quoteOrderQty=str(TRADE_AMOUNT_USD),
                )
            else:
                order = self.client.order_market_sell(
                    symbol=SYMBOL, quantity=quantity_str
                )
            print_colored("✅ Order executed successfully", Colors.SUCCESS, "bold")
            self._display_order_summary(order)
            return {"status": "SUCCESS", "order": order}
        except BinanceAPIException as e:
            error_msg = f"Binance API error: {e.code} - {e.message}"
            print_colored(error_msg, Colors.ERROR, "bold")
            return {"status": "API_ERROR", "error": error_msg}
        except Exception as e:
            error_msg = f"Order execution failed: {e}"
            print_colored(error_msg, Colors.ERROR, "bold")
            return {"status": "ERROR", "error": error_msg}

    def _execute_test_order(self, side, token_amount, current_price):
        print_colored("📋 TEST MODE - Simulating order", Colors.WARNING, "bold")
        simulated_order = {
            "orderId": int(time.time() * 1000),
            "symbol": SYMBOL,
            "side": side.upper(),
            "type": "MARKET",
            "origQty": str(token_amount),
            "executedQty": str(token_amount),
            "cummulativeQuoteQty": str(token_amount * current_price),
            "status": "FILLED",
            "fills": [
                {
                    "price": str(current_price),
                    "qty": str(token_amount),
                    "commission": "0.00000000",
                    "commissionAsset": PAIR if side == "BUY" else TOKEN,
                }
            ],
            "transactTime": int(time.time() * 1000),
        }
        self._display_order_summary(simulated_order)
        return {"status": "TEST_SUCCESS", "order": simulated_order}

    def _display_order_summary(self, order):
        table = Table(show_header=False, box=None)
        table.add_column(style=Colors.INFO, width=20)
        table.add_column(style=Colors.WHITE)
        table.add_row("Order ID", str(order.get("orderId", "TEST")))
        table.add_row("Symbol", order.get("symbol", SYMBOL))
        table.add_row("Side", order.get("side", "N/A"))
        table.add_row("Quantity", f"{float(order.get('origQty', 0)):.6f}")
        table.add_row("Status", order.get("status", "FILLED"))
        fills = order.get("fills", [])
        if fills:
            avg_price = sum(float(f["price"]) for f in fills) / len(fills)
            table.add_row("Avg Price", f"${avg_price:.8f}")
        console.print(Panel(table, title="📊 ORDER SUMMARY", border_style=Colors.INFO))


# ============================================================================
# TRANSACTION LOGGING
# ============================================================================


def log_binance_transaction(order, action, strategy, **kwargs):
    try:
        transaction = {
            "timestamp": datetime.now().isoformat(),
            "order_id": order.get("orderId", ""),
            "client_order_id": order.get("clientOrderId", ""),
            "symbol": order.get("symbol", SYMBOL),
            "side": order.get("side", ""),
            "type": order.get("type", ""),
            "quantity": float(order.get("origQty", 0)),
            "executed_quantity": float(order.get("executedQty", 0)),
            "cumulative_quote_qty": float(order.get("cummulativeQuoteQty", 0)),
            "status": order.get("status", ""),
            "time_in_force": order.get("timeInForce", ""),
            "transact_time": order.get("transactTime", ""),
            "action": action,
            "strategy": strategy,
        }
        fills = order.get("fills", [])
        if fills:
            total_commission = 0
            weighted_price = 0
            total_qty = 0
            for fill in fills:
                price = float(fill.get("price", 0))
                qty = float(fill.get("qty", 0))
                commission = float(fill.get("commission", 0))
                total_commission += commission
                weighted_price += price * qty
                total_qty += qty
            transaction["average_price"] = (
                weighted_price / total_qty if total_qty > 0 else 0
            )
            transaction["total_commission"] = total_commission
            transaction["commission_asset"] = fills[0].get("commissionAsset", "")
        if "pnl_percent" in kwargs:
            transaction["pnl_percent"] = kwargs["pnl_percent"]
            transaction["pnl_usd"] = kwargs["pnl_usd"]
        if "exit_reason" in kwargs:
            transaction["exit_reason"] = kwargs["exit_reason"]
        file_exists = os.path.exists(TRANSACTIONS_CSV)
        with open(TRANSACTIONS_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=transaction.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(transaction)
        display_transaction_summary(transaction, enhanced=True)
        logger.info(f"Transaction logged: {action} {strategy}")
    except Exception as e:
        logger.error(f"Transaction logging error: {e}")


def display_transaction_summary(transaction, enhanced=True):
    if enhanced:
        side = transaction.get("side", "N/A")
        color = (
            Colors.SUCCESS
            if side == "BUY"
            else Colors.ERROR
            if side == "SELL"
            else Colors.INFO
        )

        trans_table = Table(show_header=False, box=ROUNDED, show_lines=True)
        trans_table.add_column(style="cyan", width=20)
        trans_table.add_column(style="bright_white")

        trans_table.add_row("Timestamp", transaction.get("timestamp", "N/A"))
        trans_table.add_row("Action", transaction.get("action", "N/A"))
        trans_table.add_row("Side", side)
        trans_table.add_row("Quantity", f"{transaction.get('quantity', 0):.6f}")
        trans_table.add_row("Avg Price", f"${transaction.get('average_price', 0):.4f}")

        if "pnl_usd" in transaction:
            pnl_color = Colors.PROFIT if transaction["pnl_usd"] > 0 else Colors.LOSS
            trans_table.add_row(
                "PnL USD", f"[{pnl_color}]${transaction['pnl_usd']:.2f}[/{pnl_color}]"
            )
            trans_table.add_row(
                "PnL %", f"[{pnl_color}]{transaction['pnl_percent']:.2f}%[/{pnl_color}]"
            )

        if "exit_reason" in transaction:
            trans_table.add_row("Exit Reason", transaction["exit_reason"])

        console.print(
            Panel(trans_table, title="💼 TRANSACTION SUMMARY", border_style=color)
        )
    else:
        print_colored(
            f"Transaction: {transaction.get('action')} {transaction.get('side')}",
            Colors.INFO,
        )
        print_colored(f"Quantity: {transaction.get('quantity'):.6f}", Colors.INFO)
        print_colored(
            f"Avg Price: ${transaction.get('average_price'):.4f}", Colors.INFO
        )
        if "pnl_usd" in transaction:
            print_colored(
                f"PnL: ${transaction['pnl_usd']:.2f} ({transaction['pnl_percent']:.2f}%)",
                Colors.INFO,
            )


# ============================================================================
# MAIN PROCESSING FUNCTIONS
# ============================================================================


def process_single_row(row):
    global trade_state
    daily_diff_str = row.get("daily_diff", "0")
    daily_diff = calculate_daily_diff(daily_diff_str)
    trade_state.current_daily_diff = daily_diff

    # Only trade if within Strategy AAA range
    if DAILY_DIFF_LOWER_LIMIT <= daily_diff <= DAILY_DIFF_UPPER_LIMIT:
        # Display strategy activation if not already displayed
        if trade_state.strategy_variant != StrategyVariant.AAA:
            trade_state.strategy_variant = StrategyVariant.AAA
            display_strategy_activation(daily_diff, enhanced=True)

        entry_setup = check_entry_setup(row)
        if entry_setup:
            print_signal(
                "ENTRY SETUP",
                f"{entry_setup.value} detected",
                Colors.MAGENTA,
                enhanced=True,
            )

            if is_entry_condition_met(row, entry_setup):
                print_signal(
                    "ENTRY CONFIRMED",
                    "All conditions satisfied",
                    Colors.SUCCESS,
                    enhanced=True,
                )
                process_entry_signal(row, entry_setup)
            else:
                waiting_key = (
                    f"waiting_{entry_setup.name.lower().split('_')[1]}_above_fibo_236"
                )
                trade_state.waiting_conditions[waiting_key] = True
                print_waiting(
                    f"{entry_setup.value.split('_')[1]} ≥ Fibo_23.6%", animate=True
                )

        check_waiting_ma_conditions(row)

        if trade_state.position_open and trade_state.entry_signal_confirmed:
            if trade_state.phase == Phase.STOP_LOSS_ACTIVE:
                execute_stop_loss_flow(row)
            else:
                monitor_strategy_aaa_exit(row)

        if (
            trade_state.mode == TradingMode.SELL
            and trade_state.position_open
            and trade_state.entry_signal_confirmed
            and trade_state.captured_fibo_0_sell is not None
        ):
            ma_200 = row.get("long200")
            if pd.notna(ma_200) and ma_200 <= trade_state.captured_fibo_0_sell:
                print_signal(
                    "SELL MODE OPTION 2 EXIT",
                    "MA_200 ≤ Captured Fibo_0",
                    Colors.ERROR,
                    enhanced=True,
                )
                execute_exit_order(row, "SELL_MODE_FIBO_0_EXIT")
    else:
        # Outside Strategy AAA range - don't process
        if VERBOSE_LOGGING:
            print_colored(
                f"Daily diff {daily_diff:+.2f}% outside AAA range, skipping", Colors.DIM
            )


def check_waiting_ma_conditions(row):
    fibo_236 = row.get("level_236")
    if pd.isna(fibo_236):
        return

    if trade_state.waiting_conditions["waiting_ma_200_above_fibo_236"]:
        ma_200 = row.get("long200")
        if not pd.isna(ma_200) and ma_200 >= fibo_236:
            trade_state.waiting_conditions["waiting_ma_200_above_fibo_236"] = False
            print_signal(
                "WAITING MET", "MA_200 ≥ Fibo_23.6%", Colors.SUCCESS, enhanced=True
            )
            if is_entry_condition_met(row, StrategyType.MA_200_WAVE):
                process_entry_signal(row, StrategyType.MA_200_WAVE)

    if trade_state.waiting_conditions["waiting_ma_350_above_fibo_236"]:
        ma_350 = row.get("long350")
        if not pd.isna(ma_350) and ma_350 >= fibo_236:
            trade_state.waiting_conditions["waiting_ma_350_above_fibo_236"] = False
            print_signal(
                "WAITING MET", "MA_350 ≥ Fibo_23.6%", Colors.SUCCESS, enhanced=True
            )
            if is_entry_condition_met(row, StrategyType.MA_350_WAVE):
                process_entry_signal(row, StrategyType.MA_350_WAVE)

    if trade_state.waiting_conditions["waiting_ma_500_above_fibo_236"]:
        ma_500 = row.get("long500")
        if not pd.isna(ma_500) and ma_500 >= fibo_236:
            trade_state.waiting_conditions["waiting_ma_500_above_fibo_236"] = False
            print_signal(
                "WAITING MET", "MA_500 ≥ Fibo_23.6%", Colors.SUCCESS, enhanced=True
            )
            if is_entry_condition_met(row, StrategyType.MA_500_WAVE):
                process_entry_signal(row, StrategyType.MA_500_WAVE)


def update_trading_mode():
    if trade_state.wallet_mgr:
        trade_state.mode = trade_state.wallet_mgr.determine_mode()
        wallet_info = {}
        if trade_state.mode == TradingMode.BUY:
            bal = trade_state.wallet_mgr.get_balance("FDUSD")
            if bal > 0:
                wallet_info["FDUSD"] = f"${bal:.2f}"
        elif trade_state.mode == TradingMode.SELL:
            sol_value = trade_state.wallet_mgr.get_token_value_usd("SOL")
            wallet_info["SOL"] = f"${sol_value:.2f}"
        display_mode_banner(trade_state.mode.value, wallet_info, enhanced=True)


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state_data = json.load(f)
            trade_state.mode = TradingMode(state_data.get("mode", "NEUTRAL"))
            trade_state.strategy_variant = StrategyVariant(
                state_data.get("strategy_variant", "NONE")
            )
            trade_state.phase = Phase(state_data.get("phase", "ENTRY_MONITORING"))
            trade_state.position_open = state_data.get("position_open", False)
            trade_state.position_size = state_data.get("position_size", 0.0)
            trade_state.entry_price = state_data.get("entry_price")
            trade_state.virtual_entry_price = state_data.get("virtual_entry_price")
            active_strat = state_data.get("active_strategy")
            trade_state.active_strategy = (
                StrategyType(active_strat) if active_strat else None
            )
            trade_state.entry_signal_confirmed = state_data.get(
                "entry_signal_confirmed", False
            )
            trade_state.stats = state_data.get("stats", trade_state.stats)
            captured = state_data.get("captured_levels", {})
            trade_state.captured_fibo_0 = captured.get("fibo_0")
            trade_state.captured_fibo_1 = captured.get("fibo_1")
            trade_state.captured_fibo_1_dip = captured.get("fibo_1_dip")
            trade_state.captured_fibo_0_sell = captured.get("fibo_0_sell")
            trade_state.waiting_conditions = state_data.get(
                "waiting_conditions", trade_state.waiting_conditions
            )

            # Load new flowchart states
            trade_state.step_3_path_active = state_data.get("step_3_path_active", False)
            trade_state.branch_b_quality_check_passed = state_data.get(
                "branch_b_quality_check_passed", False
            )
            trade_state.capture_path_active = state_data.get(
                "capture_path_active", False
            )
            trade_state.condition2_active = state_data.get("condition2_active", False)
            trade_state.condition1_active = state_data.get("condition1_active", False)
            trade_state.dual_monitoring_active = state_data.get(
                "dual_monitoring_active", False
            )
            trade_state.phase_2_check_active = state_data.get(
                "phase_2_check_active", False
            )
            trade_state.quality_check_passed = state_data.get(
                "quality_check_passed", False
            )

            last_time_str = state_data.get("last_processed_time")
            trade_state.last_processed_time = (
                datetime.fromisoformat(last_time_str) if last_time_str else None
            )
            logger.info("State loaded successfully")
        except Exception as e:
            logger.error(f"Error loading state: {e}")


def save_state():
    try:
        state_data = {
            "timestamp": datetime.now().isoformat(),
            "mode": trade_state.mode.value,
            "strategy_variant": trade_state.strategy_variant.value,
            "phase": trade_state.phase.value,
            "position_open": trade_state.position_open,
            "position_size": trade_state.position_size,
            "entry_price": trade_state.entry_price,
            "virtual_entry_price": trade_state.virtual_entry_price,
            "active_strategy": trade_state.active_strategy.value
            if trade_state.active_strategy
            else None,
            "entry_signal_confirmed": trade_state.entry_signal_confirmed,
            "stats": trade_state.stats,
            "captured_levels": {
                "fibo_0": trade_state.captured_fibo_0,
                "fibo_1": trade_state.captured_fibo_1,
                "fibo_1_dip": trade_state.captured_fibo_1_dip,
                "fibo_0_sell": trade_state.captured_fibo_0_sell,
            },
            "waiting_conditions": trade_state.waiting_conditions,
            # Save new flowchart states
            "step_3_path_active": trade_state.step_3_path_active,
            "branch_b_quality_check_passed": trade_state.branch_b_quality_check_passed,
            "capture_path_active": trade_state.capture_path_active,
            "condition2_active": trade_state.condition2_active,
            "condition1_active": trade_state.condition1_active,
            "dual_monitoring_active": trade_state.dual_monitoring_active,
            "phase_2_check_active": trade_state.phase_2_check_active,
            "quality_check_passed": trade_state.quality_check_passed,
            "last_processed_time": str(trade_state.last_processed_time)
            if trade_state.last_processed_time
            else None,
        }
        with open(STATE_FILE, "w") as f:
            json.dump(state_data, f, indent=2)
        logger.info(f"State saved to {STATE_FILE}")
    except Exception as e:
        logger.error(f"Error saving state: {e}")


def initialize_client():
    try:
        api_key = os.environ.get("BINANCE_API_KEY")
        api_secret = os.environ.get("BINANCE_API_SECRET")
        if not api_key or not api_secret:
            print_colored(
                "Binance API keys not found in environment",
                Colors.ERROR,
                "bold",
                animate=True,
            )
            print_colored("Set them with:", Colors.WARNING, animate=True)
            print_colored(
                "  export BINANCE_API_KEY='your_key'", Colors.INFO, animate=True
            )
            print_colored(
                "  export BINANCE_API_SECRET='your_secret'", Colors.INFO, animate=True
            )
            return None
        client = Client(api_key, api_secret)
        client.ping()
        print_colored(
            "✅ Binance connection successful", Colors.SUCCESS, "bold", animate=True
        )
        return client
    except Exception as e:
        print_colored(
            f"❌ Binance connection failed: {e}", Colors.ERROR, "bold", animate=True
        )
        return None


# ============================================================================
# MAIN LOOP
# ============================================================================


def main():
    global client, trade_state, running, order_executor, csv_monitor, safety_manager

    display_dino("Look_alive", "startup", "TRADING BOT INITIALIZING", enhanced=True)

    client = initialize_client()
    if not client and not TEST_MODE:
        display_dino("Trouble", "dead", "INITIALIZATION ERROR", enhanced=True)
        sys.exit(1)

    safety_manager = SafetyManager()
    wallet_manager = WalletManager(client)
    trade_state.wallet_mgr = wallet_manager
    order_executor = OrderExecutor(client, safety_manager, wallet_manager)
    csv_monitor = CSVMonitor(INDICATORS_CSV)

    load_state()
    update_trading_mode()

    print_colored("Monitoring CSV for updates...", Colors.INFO, "bold")
    VisualEffects.display_spinner("Starting trading loop", Colors.ACTIVE)

    while running:
        if csv_monitor.check_update():
            print_colored(
                "CSV updated - processing new data", Colors.SUCCESS, animate=True
            )
            rows = csv_monitor.get_latest_rows(5 if CSV_BATCH_PROCESSING else 1)
            for _, row in rows.iterrows():
                process_single_row(row)
                trade_state.last_processed_time = datetime.now()
            save_state()

        if time.time() % STATUS_UPDATE_INTERVAL == 0:
            display_status(csv_monitor, enhanced=True)

        time.sleep(CSV_UPDATE_CHECK_INTERVAL)

    print_colored("Trading bot stopped", Colors.WARNING, "bold")


# GLOBAL VARIABLES
trade_state = TradeState()
client = None
running = True
state_lock = threading.Lock()
console = Console()
order_executor = None
csv_monitor = None
safety_manager = None

# ENTRY POINT
if __name__ == "__main__":
    if not TEST_MODE:
        if not os.environ.get("BINANCE_API_KEY") or not os.environ.get(
            "BINANCE_API_SECRET"
        ):
            display_dino(
                "API KEYS REQUIRED FOR REAL TRADING",
                "trex_roar",
                "CONFIGURATION ERROR",
                enhanced=True,
            )
            print_colored(
                "Set environment variables or enable TEST_MODE",
                Colors.ERROR,
                "bold",
                animate=True,
            )
            os._exit(1)
    try:
        main()
    except Exception as e:
        display_dino(
            f"Fatal error: {str(e)[:100]}", "dead", "FATAL ERROR", enhanced=True
        )
        logger.exception("Fatal error")
        os._exit(1)
